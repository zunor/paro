// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Writer-side sparse vector inline artifact builder.

use std::collections::HashSet;

use crate::index::sparse::SparseIndexBuilder;
use crate::rowset::SparseVector;
use crate::tablet::ColumnId;
use paro_common::error::{self as paro_error, Result};

use crate::search::capability::SearchIndexKind;
use crate::search::inline_sink::{
    InlineArtifactBlob, InlineArtifactBuildResult, InlineArtifactBuilder, SearchStatsDelta,
    SegmentChunkInput, SegmentChunkSink, SegmentFlushCtx, SegmentSinkSavepoint, SparseStatsDelta,
};
use crate::search::providers::fulltext::inline::{column_position, visit_varlen_column_rows};
use crate::search::providers::sparse::row_image::{
    decode_sparse_row_value, validate_sparse_binary_row_image_column,
};
use crate::search::stats::{SearchArtifactStats, SearchProviderStats, SparseProviderStats};

#[derive(Debug, Default)]
pub struct SparseInlineArtifactBuilder;

impl InlineArtifactBuilder for SparseInlineArtifactBuilder {
    fn open_sink(&self, ctx: &SegmentFlushCtx<'_>) -> Result<Box<dyn SegmentChunkSink>> {
        if ctx.definition.kind != SearchIndexKind::Sparse {
            return Err(paro_error::invalid_input(
                "SparseInlineArtifactBuilder requires a Sparse definition",
            ));
        }
        let column_id =
            ctx.definition.column_ids.first().copied().ok_or_else(|| {
                paro_error::invalid_input("Sparse definition is missing column id")
            })?;
        let full_schema_position = ctx
            .column_schema
            .iter()
            .enumerate()
            .find_map(|(position, column)| (column.id == column_id).then_some((position, column)))
            .map(|(position, column)| {
                validate_sparse_binary_row_image_column(
                    &column.logical_type,
                    &ctx.definition.provider_config,
                )
                .map(|()| position)
            })
            .transpose()?
            .ok_or_else(|| {
                paro_error::invalid_input(format!(
                    "Sparse column {} is not present in writer schema",
                    column_id
                ))
            })?;
        Ok(Box::new(SparseInlineSink {
            definition_id: ctx.definition.definition_id,
            generation_id: ctx.generation_id,
            column_id,
            full_schema_position,
            builder: SparseIndexBuilder::new(),
            rollback_log: Vec::new(),
            savepoints: Vec::new(),
            next_savepoint_id: 1,
            remap_scratch: SparseVector::default(),
            dimensions: HashSet::new(),
            rows_seen: 0,
            nnz: 0,
            l2_norm_sum: 0.0,
            max_l2_norm: 0.0,
        }))
    }
}

struct SparseInlineSink {
    definition_id: u64,
    generation_id: u64,
    column_id: ColumnId,
    full_schema_position: usize,
    builder: SparseIndexBuilder,
    rollback_log: Vec<(u32, SparseVector)>,
    savepoints: Vec<SparseRollbackCheckpoint>,
    next_savepoint_id: u64,
    remap_scratch: SparseVector,
    dimensions: HashSet<u32>,
    rows_seen: u64,
    nnz: u64,
    l2_norm_sum: f64,
    max_l2_norm: f32,
}

impl SegmentChunkSink for SparseInlineSink {
    fn append_chunk(&mut self, input: SegmentChunkInput<'_>) -> Result<()> {
        let column_position = column_position(input, self.column_id, self.full_schema_position)?;
        let column = input.columns.get(column_position).ok_or_else(|| {
            paro_error::invalid_input(format!(
                "Sparse column {} is missing from chunk",
                self.column_id
            ))
        })?;
        if !self.savepoints.is_empty() {
            self.rollback_log.reserve(column.num_values as usize);
        }
        visit_varlen_column_rows(column, |row_offset, value| {
            let Some(value) = value else {
                return Ok(());
            };
            let doc_id = input.base_row_id.checked_add(row_offset).ok_or_else(|| {
                paro_error::out_of_range("Sparse inline doc id exceeds u32 range")
            })?;
            let vector = decode_sparse_row_value(value, self.column_id, doc_id)?;
            self.append_vector(doc_id, vector)?;
            Ok(())
        })?;
        self.rows_seen = self
            .rows_seen
            .checked_add(u64::from(column.num_values))
            .ok_or_else(|| paro_error::out_of_range("Sparse inline row count overflow"))?;
        Ok(())
    }

    fn mark_savepoint(&mut self) -> Result<SegmentSinkSavepoint> {
        let state_id = self.next_savepoint_id;
        self.next_savepoint_id = self.next_savepoint_id.saturating_add(1);
        self.savepoints.push(SparseRollbackCheckpoint {
            state_id,
            rows_seen: self.rows_seen,
            nnz: self.nnz,
            l2_norm_sum: self.l2_norm_sum,
            max_l2_norm: self.max_l2_norm,
            dimensions: self.dimensions.clone(),
            rollback_entry_count: self.rollback_log.len(),
            next_doc_id: self.builder.next_doc_id(),
        });
        Ok(SegmentSinkSavepoint {
            rows_seen: self.rows_seen,
            bytes_buffered: self.nnz,
            entries_seen: self.rollback_log.len() as u64,
            state_id,
        })
    }

    fn rollback_to_savepoint(&mut self, savepoint: &SegmentSinkSavepoint) -> Result<()> {
        if savepoint.rows_seen > self.rows_seen {
            return Err(paro_error::invalid_input(format!(
                "Sparse inline sink savepoint row {} is beyond current row {}",
                savepoint.rows_seen, self.rows_seen
            )));
        }
        let rollback_entry_count = usize::try_from(savepoint.entries_seen).map_err(|_| {
            paro_error::out_of_range("Sparse inline savepoint entries exceed usize")
        })?;
        if rollback_entry_count > self.rollback_log.len() {
            return Err(paro_error::invalid_input(format!(
                "Sparse inline sink savepoint entry {} is beyond current entry {}",
                rollback_entry_count,
                self.rollback_log.len()
            )));
        }
        let Some(checkpoint) = self
            .savepoints
            .iter()
            .rev()
            .find(|checkpoint| {
                checkpoint.state_id == savepoint.state_id
                    && checkpoint.rows_seen == savepoint.rows_seen
                    && checkpoint.rollback_entry_count == rollback_entry_count
            })
            .cloned()
        else {
            return Err(paro_error::invalid_input(
                "Sparse inline sink savepoint is not active",
            ));
        };
        let removed_vectors = self.rollback_log.split_off(checkpoint.rollback_entry_count);
        for (doc_id, vector) in removed_vectors {
            self.builder.remove(doc_id, &vector)?;
        }
        self.builder.set_next_doc_id(checkpoint.next_doc_id);
        self.rows_seen = checkpoint.rows_seen;
        self.nnz = checkpoint.nnz;
        self.l2_norm_sum = checkpoint.l2_norm_sum;
        self.max_l2_norm = checkpoint.max_l2_norm;
        self.dimensions = checkpoint.dimensions;
        self.savepoints
            .retain(|active| active.rollback_entry_count <= checkpoint.rollback_entry_count);
        Ok(())
    }

    fn finish(self: Box<Self>) -> Result<InlineArtifactBuildResult> {
        let provider_stats = SparseProviderStats {
            row_count: self.rows_seen,
            nnz: self.nnz,
            posting_fanout: self.nnz,
            unique_dimensions: self.dimensions.len() as u64,
            avg_vector_nnz: if self.rows_seen == 0 {
                0.0
            } else {
                self.nnz as f32 / self.rows_seen as f32
            },
            l2_norm_sum: self.l2_norm_sum,
            max_l2_norm: self.max_l2_norm,
        };
        let stats_delta = SparseStatsDelta {
            row_count: self.rows_seen,
            nnz: self.nnz,
            posting_fanout: self.nnz,
            unique_dimensions: provider_stats.unique_dimensions,
            l2_norm_sum: self.l2_norm_sum,
            max_l2_norm: self.max_l2_norm,
        };
        let index = self.builder.build();
        let bytes = index.serialize()?;
        let checksum = seahash::hash(&bytes);
        let bytes_on_disk = bytes.len() as u64;
        Ok(InlineArtifactBuildResult {
            blobs: vec![InlineArtifactBlob {
                definition_id: self.definition_id,
                generation_id: self.generation_id,
                column_id: self.column_id,
                kind: SearchIndexKind::Sparse,
                bytes,
                stats: SearchArtifactStats {
                    row_count: self.rows_seen,
                    bytes_on_disk,
                    provider_stats: Some(SearchProviderStats::Sparse(provider_stats)),
                },
                checksum,
            }],
            stats_delta: Some(SearchStatsDelta::Sparse(stats_delta)),
        })
    }
}

impl SparseInlineSink {
    fn append_vector(&mut self, doc_id: u32, vector: SparseVector) -> Result<()> {
        self.nnz = self
            .nnz
            .checked_add(vector.len() as u64)
            .ok_or_else(|| paro_error::out_of_range("Sparse inline nnz overflow"))?;
        let l2_norm = sparse_l2_norm(&vector);
        self.l2_norm_sum += f64::from(l2_norm);
        self.max_l2_norm = self.max_l2_norm.max(l2_norm);
        self.dimensions.reserve(vector.len());
        self.dimensions.extend(vector.dims.iter().copied());
        self.builder
            .add_with_remap_scratch(doc_id, &vector, &mut self.remap_scratch)?;
        if !self.savepoints.is_empty() {
            self.rollback_log.push((doc_id, vector));
        }
        Ok(())
    }
}

#[derive(Clone)]
struct SparseRollbackCheckpoint {
    state_id: u64,
    rows_seen: u64,
    nnz: u64,
    l2_norm_sum: f64,
    max_l2_norm: f32,
    dimensions: HashSet<u32>,
    rollback_entry_count: usize,
    next_doc_id: u32,
}

fn sparse_l2_norm(vector: &SparseVector) -> f32 {
    vector
        .weights
        .iter()
        .map(|weight| weight * weight)
        .sum::<f32>()
        .sqrt()
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use serde_json::json;
    use tempfile::TempDir;

    use super::*;
    use crate::index::sparse::SparseVectorIndex;
    use crate::search::providers::sparse::row_image::encode_sparse_row_image;
    use crate::search::{FlushSearchMode, SearchFreshnessPolicy, SearchIndexDefinition};
    use crate::tablet::TabletColumn;
    use paro_common::types::LogicalType;

    #[test]
    fn sparse_inline_sink_consumes_writer_chunk_without_segment_reader() {
        let definition = SearchIndexDefinition {
            definition_id: 17,
            table_id: 11,
            name: "sparse_idx".to_string(),
            kind: SearchIndexKind::Sparse,
            column_ids: vec![1],
            expression: None,
            provider_config: json!({}),
            freshness_policy: SearchFreshnessPolicy::default_for_kind(SearchIndexKind::Sparse),
            config_fingerprint: 99,
        };
        let columns_schema = vec![
            TabletColumn::new(0, "id", LogicalType::Integer),
            TabletColumn::new(1, "sparse_vec", LogicalType::Blob),
        ];
        let temp_dir = TempDir::new().unwrap();
        let ctx = SegmentFlushCtx {
            rowset_id: 42,
            segment_id: 0,
            definition: &definition,
            generation_id: 19,
            flush_mode: FlushSearchMode::InlineRequired,
            admission: None,
            staging_dir: Path::new(temp_dir.path()),
            column_schema: &columns_schema,
        };

        let builder = SparseInlineArtifactBuilder;
        let mut sink = builder.open_sink(&ctx).unwrap();
        let first =
            encode_sparse_row_image(&SparseVector::new(vec![1, 3], vec![1.0, 0.5]).unwrap())
                .unwrap();
        let second =
            encode_sparse_row_image(&SparseVector::new(vec![2], vec![1.0]).unwrap()).unwrap();
        let sparse_column = crate::rowset::ColumnData::new(
            encode_varlen_bytes(&[first.as_slice(), second.as_slice()]),
            2,
        );
        sink.append_chunk(SegmentChunkInput {
            base_row_id: 4,
            columns: &[sparse_column],
            column_ids: Some(&[1]),
        })
        .unwrap();

        let result = sink.finish().unwrap();
        let blob = result.blobs.single().expect("one sparse blob");
        assert_eq!(blob.definition_id, 17);
        assert_eq!(blob.generation_id, 19);
        assert_eq!(blob.column_id, 1);
        assert_eq!(blob.stats.row_count, 2);
        assert!(blob.stats.bytes_on_disk > 0);
        assert!(matches!(
            &blob.stats.provider_stats,
            Some(SearchProviderStats::Sparse(SparseProviderStats {
                row_count: 2,
                nnz: 3,
                posting_fanout: 3,
                unique_dimensions: 3,
                ..
            }))
        ));
        assert!(matches!(
            result.stats_delta,
            Some(SearchStatsDelta::Sparse(SparseStatsDelta {
                row_count: 2,
                nnz: 3,
                posting_fanout: 3,
                unique_dimensions: 3,
                ..
            }))
        ));

        let index = SparseVectorIndex::deserialize(&blob.bytes).unwrap();
        assert_eq!(index.num_vectors(), 6);
    }

    #[test]
    fn sparse_inline_sink_consumes_typed_binary_row_image_without_text_parse() {
        let definition = SearchIndexDefinition {
            definition_id: 21,
            table_id: 11,
            name: "sparse_idx".to_string(),
            kind: SearchIndexKind::Sparse,
            column_ids: vec![1],
            expression: None,
            provider_config: json!({}),
            freshness_policy: SearchFreshnessPolicy::default_for_kind(SearchIndexKind::Sparse),
            config_fingerprint: 99,
        };
        let columns_schema = vec![
            TabletColumn::new(0, "id", LogicalType::Integer),
            TabletColumn::new(1, "sparse_vec", LogicalType::Blob),
        ];
        let temp_dir = TempDir::new().unwrap();
        let ctx = SegmentFlushCtx {
            rowset_id: 42,
            segment_id: 0,
            definition: &definition,
            generation_id: 19,
            flush_mode: FlushSearchMode::InlineRequired,
            admission: None,
            staging_dir: Path::new(temp_dir.path()),
            column_schema: &columns_schema,
        };

        let first =
            encode_sparse_row_image(&SparseVector::new(vec![7, 1], vec![0.75, 1.0]).unwrap())
                .unwrap();
        let second =
            encode_sparse_row_image(&SparseVector::new(vec![2], vec![2.0]).unwrap()).unwrap();
        let builder = SparseInlineArtifactBuilder;
        let mut sink = builder.open_sink(&ctx).unwrap();
        let sparse_column = crate::rowset::ColumnData::new(
            encode_varlen_bytes(&[first.as_slice(), second.as_slice()]),
            2,
        );
        sink.append_chunk(SegmentChunkInput {
            base_row_id: 0,
            columns: &[sparse_column],
            column_ids: Some(&[1]),
        })
        .unwrap();

        let result = sink.finish().unwrap();
        assert!(matches!(
            result.stats_delta,
            Some(SearchStatsDelta::Sparse(SparseStatsDelta {
                row_count: 2,
                nnz: 3,
                posting_fanout: 3,
                unique_dimensions: 3,
                ..
            }))
        ));
        let blob = result.blobs.single().expect("one sparse blob");
        let index = SparseVectorIndex::deserialize(&blob.bytes).unwrap();
        let rows = index
            .search(&SparseVector::new(vec![1], vec![1.0]).unwrap(), 2, None)
            .unwrap();
        assert_eq!(rows[0].idx, 0);
    }

    #[test]
    fn sparse_inline_sink_rolls_back_to_savepoint() {
        let definition = SearchIndexDefinition {
            definition_id: 18,
            table_id: 11,
            name: "sparse_idx".to_string(),
            kind: SearchIndexKind::Sparse,
            column_ids: vec![1],
            expression: None,
            provider_config: json!({}),
            freshness_policy: SearchFreshnessPolicy::default_for_kind(SearchIndexKind::Sparse),
            config_fingerprint: 99,
        };
        let columns_schema = vec![
            TabletColumn::new(0, "id", LogicalType::Integer),
            TabletColumn::new(1, "sparse_vec", LogicalType::Blob),
        ];
        let temp_dir = TempDir::new().unwrap();
        let ctx = SegmentFlushCtx {
            rowset_id: 42,
            segment_id: 0,
            definition: &definition,
            generation_id: 19,
            flush_mode: FlushSearchMode::InlineRequired,
            admission: None,
            staging_dir: Path::new(temp_dir.path()),
            column_schema: &columns_schema,
        };

        let builder = SparseInlineArtifactBuilder;
        let mut sink = builder.open_sink(&ctx).unwrap();
        let first =
            encode_sparse_row_image(&SparseVector::new(vec![1], vec![1.0]).unwrap()).unwrap();
        sink.append_chunk(SegmentChunkInput {
            base_row_id: 0,
            columns: &[crate::rowset::ColumnData::new(
                encode_varlen_bytes(&[first.as_slice()]),
                1,
            )],
            column_ids: Some(&[1]),
        })
        .unwrap();
        let savepoint = sink.mark_savepoint().unwrap();
        let second =
            encode_sparse_row_image(&SparseVector::new(vec![2], vec![1.0]).unwrap()).unwrap();
        sink.append_chunk(SegmentChunkInput {
            base_row_id: 1,
            columns: &[crate::rowset::ColumnData::new(
                encode_varlen_bytes(&[second.as_slice()]),
                1,
            )],
            column_ids: Some(&[1]),
        })
        .unwrap();
        sink.rollback_to_savepoint(&savepoint).unwrap();

        let result = sink.finish().unwrap();
        let blob = result.blobs.single().expect("one sparse blob");
        assert_eq!(blob.stats.row_count, 1);
        assert!(matches!(
            result.stats_delta,
            Some(SearchStatsDelta::Sparse(SparseStatsDelta {
                row_count: 1,
                nnz: 1,
                posting_fanout: 1,
                unique_dimensions: 1,
                ..
            }))
        ));

        let index = SparseVectorIndex::deserialize(&blob.bytes).unwrap();
        assert_eq!(index.num_vectors(), 1);
        assert_eq!(index.get_posting_list(1).unwrap().len(), 1);
        assert!(index.get_posting_list(2).is_none());
    }

    fn encode_varlen_bytes(values: &[&[u8]]) -> Vec<u8> {
        let mut bytes = Vec::new();
        for value in values {
            bytes.extend_from_slice(&(value.len() as u32).to_le_bytes());
            bytes.extend_from_slice(value);
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
