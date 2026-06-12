// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Writer-side FullText inline artifact builder.

use crate::index::fulltext::text_index::{FullTextIndex, FullTextIndexConfig};
use crate::index::fulltext::tokenizer::{Token, TokenizerKind};
use crate::statistics::FullTextIndexStatistics;
use crate::tablet::ColumnId;
use paro_common::error::{self as paro_error, Result};

use crate::search::capability::SearchIndexKind;
use crate::search::inline_sink::{
    FullTextStatsDelta, InlineArtifactBlob, InlineArtifactBuildResult, InlineArtifactBuilder,
    SearchStatsDelta, SegmentChunkInput, SegmentChunkSink, SegmentFlushCtx, SegmentSinkSavepoint,
};
use crate::search::stats::{FullTextProviderStats, SearchArtifactStats, SearchProviderStats};

#[derive(Debug, Default)]
pub struct FullTextInlineArtifactBuilder;

impl InlineArtifactBuilder for FullTextInlineArtifactBuilder {
    fn open_sink(&self, ctx: &SegmentFlushCtx<'_>) -> Result<Box<dyn SegmentChunkSink>> {
        if ctx.definition.kind != SearchIndexKind::FullText {
            return Err(paro_error::invalid_input(
                "FullTextInlineArtifactBuilder requires a FullText definition",
            ));
        }
        let column_id =
            ctx.definition.column_ids.first().copied().ok_or_else(|| {
                paro_error::invalid_input("FullText definition is missing column id")
            })?;
        let full_schema_position = ctx
            .column_schema
            .iter()
            .position(|column| column.id == column_id)
            .ok_or_else(|| {
                paro_error::invalid_input(format!(
                    "FullText column {} is not present in writer schema",
                    column_id
                ))
            })?;
        let config = ctx
            .definition
            .provider_config
            .get("config")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("simple");
        let tokenizer_kind = TokenizerKind::from_config(config)?;
        Ok(Box::new(FullTextInlineSink {
            definition_id: ctx.definition.definition_id,
            generation_id: ctx.generation_id,
            column_id,
            full_schema_position,
            index: FullTextIndex::new_with_tokenizer_kind(
                tokenizer_kind,
                FullTextIndexConfig::default(),
            ),
            indexed_doc_ids: Vec::new(),
            token_buffer: Vec::new(),
            rows_seen: 0,
        }))
    }
}

struct FullTextInlineSink {
    definition_id: u64,
    generation_id: u64,
    column_id: ColumnId,
    full_schema_position: usize,
    index: FullTextIndex,
    indexed_doc_ids: Vec<u32>,
    token_buffer: Vec<Token>,
    rows_seen: u64,
}

impl SegmentChunkSink for FullTextInlineSink {
    fn append_chunk(&mut self, input: SegmentChunkInput<'_>) -> Result<()> {
        let column_position = column_position(input, self.column_id, self.full_schema_position)?;
        let column = input.columns.get(column_position).ok_or_else(|| {
            paro_error::invalid_input(format!(
                "FullText column {} is missing from chunk",
                self.column_id
            ))
        })?;
        self.indexed_doc_ids.reserve(column.num_values as usize);
        visit_varlen_column_rows(column, |row_offset, value| {
            let Some(value) = value else {
                return Ok(());
            };
            let doc_id = input.base_row_id.checked_add(row_offset).ok_or_else(|| {
                paro_error::out_of_range("FullText inline doc id exceeds u32 range")
            })?;
            let text = std::str::from_utf8(value).map_err(|err| {
                paro_error::data_corrupted(format!(
                    "FullText inline utf8 decode failed at column {} row {}: {}",
                    self.column_id, doc_id, err
                ))
            })?;
            self.index.add_document_with_token_buffer_deferred_prefix(
                doc_id,
                text,
                &mut self.token_buffer,
            )?;
            self.indexed_doc_ids.push(doc_id);
            Ok(())
        })?;
        self.rows_seen = self
            .rows_seen
            .checked_add(u64::from(column.num_values))
            .ok_or_else(|| paro_error::out_of_range("FullText inline row count overflow"))?;
        Ok(())
    }

    fn mark_savepoint(&mut self) -> Result<SegmentSinkSavepoint> {
        Ok(SegmentSinkSavepoint {
            rows_seen: self.rows_seen,
            bytes_buffered: 0,
            entries_seen: self.indexed_doc_ids.len() as u64,
            state_id: 0,
        })
    }

    fn rollback_to_savepoint(&mut self, savepoint: &SegmentSinkSavepoint) -> Result<()> {
        if savepoint.rows_seen > self.rows_seen {
            return Err(paro_error::invalid_input(format!(
                "FullText inline sink savepoint row {} is beyond current row {}",
                savepoint.rows_seen, self.rows_seen
            )));
        }
        let entries_seen = usize::try_from(savepoint.entries_seen).map_err(|_| {
            paro_error::out_of_range("FullText inline savepoint entries exceed usize")
        })?;
        if entries_seen > self.indexed_doc_ids.len() {
            return Err(paro_error::invalid_input(format!(
                "FullText inline sink savepoint entry {} is beyond current entry {}",
                entries_seen,
                self.indexed_doc_ids.len()
            )));
        }
        for doc_id in self.indexed_doc_ids.drain(entries_seen..) {
            self.index.remove_document(doc_id);
        }
        self.rows_seen = savepoint.rows_seen;
        Ok(())
    }

    fn finish(self: Box<Self>) -> Result<InlineArtifactBuildResult> {
        let stats = FullTextIndexStatistics::collect(&self.index);
        let provider_stats = FullTextProviderStats::from(&stats);
        let bytes = self.index.serialize()?;
        let checksum = seahash::hash(&bytes);
        let bytes_on_disk = bytes.len() as u64;
        Ok(InlineArtifactBuildResult {
            blobs: vec![InlineArtifactBlob {
                definition_id: self.definition_id,
                generation_id: self.generation_id,
                column_id: self.column_id,
                kind: SearchIndexKind::FullText,
                bytes,
                stats: SearchArtifactStats {
                    row_count: self.rows_seen,
                    bytes_on_disk,
                    provider_stats: Some(SearchProviderStats::FullText(provider_stats.clone())),
                },
                checksum,
            }],
            stats_delta: Some(SearchStatsDelta::FullText(FullTextStatsDelta {
                stats: provider_stats,
            })),
        })
    }
}

pub(crate) fn column_position(
    input: SegmentChunkInput<'_>,
    column_id: ColumnId,
    full_schema_position: usize,
) -> Result<usize> {
    if let Some(column_ids) = input.column_ids {
        return column_ids
            .iter()
            .position(|candidate| *candidate == column_id)
            .ok_or_else(|| {
                paro_error::invalid_input(format!(
                    "FullText column {} is not present in partial chunk",
                    column_id
                ))
            });
    }
    Ok(full_schema_position)
}

pub(crate) fn visit_varlen_column_rows<F>(
    column: &crate::rowset::ColumnData,
    mut visitor: F,
) -> Result<()>
where
    F: FnMut(u32, Option<&[u8]>) -> Result<()>,
{
    let mut offset = 0usize;
    let data = column.data.as_ref();
    let null_flags = column.null_flags.as_deref();
    for row_idx in 0..column.num_values {
        let len_end = offset
            .checked_add(4)
            .ok_or_else(|| paro_error::data_corrupted("varlen column offset overflow"))?;
        if len_end > data.len() {
            return Err(paro_error::data_corrupted(
                "varlen column ended before row length prefix",
            ));
        }
        let len = u32::from_le_bytes(data[offset..len_end].try_into().unwrap()) as usize;
        let value_end = len_end
            .checked_add(len)
            .ok_or_else(|| paro_error::data_corrupted("varlen column value overflow"))?;
        if value_end > data.len() {
            return Err(paro_error::data_corrupted(
                "varlen column row extends past chunk payload",
            ));
        }
        let value = if is_null_at(null_flags, row_idx as usize) {
            None
        } else {
            Some(&data[len_end..value_end])
        };
        visitor(row_idx, value)?;
        offset = value_end;
    }
    if offset != data.len() {
        return Err(paro_error::data_corrupted(
            "varlen column has trailing bytes after declared rows",
        ));
    }
    Ok(())
}

fn is_null_at(flags: Option<&[u8]>, row_idx: usize) -> bool {
    let Some(flags) = flags else {
        return false;
    };
    let byte_idx = row_idx / 8;
    let bit_idx = row_idx % 8;
    flags
        .get(byte_idx)
        .is_some_and(|byte| ((byte >> bit_idx) & 1) == 1)
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use bytes::Bytes;
    use serde_json::json;
    use tempfile::TempDir;

    use super::*;
    use crate::index::fulltext::text_index::FullTextIndex;
    use crate::search::{
        FlushSearchMode, SearchFreshnessPolicy, SearchIndexDefinition, SearchProviderStats,
    };
    use crate::tablet::TabletColumn;
    use paro_common::types::LogicalType;

    #[test]
    fn fulltext_inline_sink_consumes_writer_chunk_without_segment_reader() {
        let definition = SearchIndexDefinition {
            definition_id: 7,
            table_id: 11,
            name: "body_fts".to_string(),
            kind: SearchIndexKind::FullText,
            column_ids: vec![1],
            expression: None,
            provider_config: json!({"config": "simple"}),
            freshness_policy: SearchFreshnessPolicy::default_for_kind(SearchIndexKind::FullText),
            config_fingerprint: 99,
        };
        let columns_schema = vec![
            TabletColumn::new(0, "id", LogicalType::Integer),
            TabletColumn::new(1, "body", LogicalType::Varchar),
        ];
        let temp_dir = TempDir::new().unwrap();
        let ctx = SegmentFlushCtx {
            rowset_id: 42,
            segment_id: 0,
            definition: &definition,
            generation_id: 13,
            flush_mode: FlushSearchMode::InlineRequired,
            admission: None,
            staging_dir: Path::new(temp_dir.path()),
            column_schema: &columns_schema,
        };

        let builder = FullTextInlineArtifactBuilder;
        let mut sink = builder.open_sink(&ctx).unwrap();
        let body = encode_varlen(&["vector database", "", "graph search"]);
        let body_column = crate::rowset::ColumnData::with_nulls(body, vec![0b0000_0010], 3);
        sink.append_chunk(SegmentChunkInput {
            base_row_id: 10,
            columns: &[body_column],
            column_ids: Some(&[1]),
        })
        .unwrap();

        let result = sink.finish().unwrap();
        let blob = result.blobs.single().expect("one fulltext blob");
        assert_eq!(blob.definition_id, 7);
        assert_eq!(blob.generation_id, 13);
        assert_eq!(blob.column_id, 1);
        assert_eq!(blob.stats.row_count, 3);
        assert!(blob.stats.bytes_on_disk > 0);
        assert!(matches!(
            blob.stats.provider_stats,
            Some(SearchProviderStats::FullText(_))
        ));
        assert!(matches!(
            result.stats_delta,
            Some(SearchStatsDelta::FullText(_))
        ));

        let index = FullTextIndex::deserialize(&blob.bytes).unwrap();
        let stats = FullTextIndexStatistics::collect(&index);
        assert_eq!(stats.total_docs, 2);
        assert_eq!(stats.tokenizer_kind, TokenizerKind::Default);
    }

    #[test]
    fn fulltext_inline_sink_rolls_back_to_savepoint() {
        let definition = SearchIndexDefinition {
            definition_id: 8,
            table_id: 11,
            name: "body_fts".to_string(),
            kind: SearchIndexKind::FullText,
            column_ids: vec![1],
            expression: None,
            provider_config: json!({"config": "simple"}),
            freshness_policy: SearchFreshnessPolicy::default_for_kind(SearchIndexKind::FullText),
            config_fingerprint: 99,
        };
        let columns_schema = vec![
            TabletColumn::new(0, "id", LogicalType::Integer),
            TabletColumn::new(1, "body", LogicalType::Varchar),
        ];
        let temp_dir = TempDir::new().unwrap();
        let ctx = SegmentFlushCtx {
            rowset_id: 42,
            segment_id: 0,
            definition: &definition,
            generation_id: 13,
            flush_mode: FlushSearchMode::InlineRequired,
            admission: None,
            staging_dir: Path::new(temp_dir.path()),
            column_schema: &columns_schema,
        };

        let builder = FullTextInlineArtifactBuilder;
        let mut sink = builder.open_sink(&ctx).unwrap();
        sink.append_chunk(SegmentChunkInput {
            base_row_id: 0,
            columns: &[crate::rowset::ColumnData::new(
                encode_varlen(&["alpha keep"]),
                1,
            )],
            column_ids: Some(&[1]),
        })
        .unwrap();
        let savepoint = sink.mark_savepoint().unwrap();
        sink.append_chunk(SegmentChunkInput {
            base_row_id: 1,
            columns: &[crate::rowset::ColumnData::new(
                encode_varlen(&["rollback only"]),
                1,
            )],
            column_ids: Some(&[1]),
        })
        .unwrap();
        sink.rollback_to_savepoint(&savepoint).unwrap();

        let result = sink.finish().unwrap();
        let blob = result.blobs.single().expect("one fulltext blob");
        assert_eq!(blob.stats.row_count, 1);

        let index = FullTextIndex::deserialize(&blob.bytes).unwrap();
        let keep = index.parse_query("keep").unwrap();
        let rollback = index.parse_query("rollback").unwrap();
        assert!(index.filter(&keep, None).contains(0));
        assert!(index.filter(&rollback, None).is_empty());
        assert_eq!(FullTextIndexStatistics::collect(&index).total_docs, 1);
    }

    #[test]
    fn varlen_chunk_reader_rejects_trailing_bytes() {
        let column = crate::rowset::ColumnData::new(Bytes::from_static(&[0, 0, 0, 0, 9]), 1);
        let err = visit_varlen_column_rows(&column, |_row, _value| Ok(())).unwrap_err();
        assert!(
            err.to_string().contains("trailing bytes"),
            "unexpected error: {err}"
        );
    }

    fn encode_varlen(values: &[&str]) -> Vec<u8> {
        let mut bytes = Vec::new();
        for value in values {
            bytes.extend_from_slice(&(value.len() as u32).to_le_bytes());
            bytes.extend_from_slice(value.as_bytes());
        }
        bytes
    }

    trait Single<T> {
        fn single(self) -> Option<T>;
    }

    impl<T> Single<T> for Vec<T> {
        fn single(mut self) -> Option<T> {
            if self.len() == 1 {
                self.pop()
            } else {
                None
            }
        }
    }
}
