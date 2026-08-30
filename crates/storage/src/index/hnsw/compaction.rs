// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use crate::compaction::execution::index_rebuild::{
    CompactionGenerationContext, CompactionIndexRebuilder,
};
use crate::compaction::plan::types::CompactionPlan;
use crate::index::hnsw::vector_storage::prepare_build_vector_storage;
use crate::index::hnsw::{
    open_plain_vector_column, GraphLayers, GraphLayersBuilder, GraphLayersHealer,
    HnswBuildContract, HnswIndex, IndexedVectorStorage, PointOffset, VectorStorage,
    HNSW_ARTIFACT_ALIGNMENT,
};
#[cfg(test)]
use crate::index::hnsw::{DistanceMetric, HnswConfig};
use crate::rowset::page::{IndexPageFooter, IndexPageType, PageFooter, PageIO, PagePointer};
use crate::rowset::segment::{Segment, SegmentFooter, SegmentOptions};
use crate::rowset::RowsetSharedPtr;
use crate::search::{SearchFreshnessPolicy, SearchIndexDefinition, SearchIndexKind};
use crate::statistics::HnswIndexStatistics;
use crate::tablet::Tablet;
use paro_common::error::{self as paro_error, Result};
use paro_common::types::LogicalType;
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::sync::Arc;

#[derive(Clone, Copy, Debug)]
struct HnswIndexedColumn {
    column_id: u32,
    dim: usize,
    build_contract: HnswBuildContract,
}

// Reuse the old graph only when most output points still map back to it.
// Otherwise a full rebuild is usually cheaper.
const MIN_HEALER_OVERLAP_RATIO: f64 = 0.95;

/// Rebuilder for HNSW indexes during compaction.
///
/// Compaction disables eager HNSW page build, so this rebuilder writes the
/// index pages afterward.
pub struct HnswIndexRebuilder;

impl HnswIndexRebuilder {
    pub fn new() -> Self {
        Self
    }

    fn collect_indexed_columns(
        generation_context: &CompactionGenerationContext,
        tablet: &Tablet,
    ) -> Result<Vec<HnswIndexedColumn>> {
        let schema = tablet
            .schema()
            .ok_or_else(|| paro_error::internal("Tablet schema not available for HNSW rebuild"))?;
        let mut indexed_columns = Vec::new();
        for definition in generation_context
            .search_definitions
            .iter()
            .filter(|definition| compaction_requires_inline_hnsw(definition))
        {
            let [column_id] = definition.column_ids.as_slice() else {
                return Err(paro_error::data_corrupted(format!(
                    "HNSW definition {} must reference exactly one column",
                    definition.definition_id
                )));
            };
            if indexed_columns
                .iter()
                .any(|column: &HnswIndexedColumn| column.column_id == *column_id)
            {
                return Err(paro_error::invalid_input(format!(
                    "compaction cannot materialize multiple active HNSW definitions for column {}",
                    column_id
                )));
            }
            let column = schema
                .columns()
                .iter()
                .find(|column| column.id == *column_id)
                .ok_or_else(|| {
                    paro_error::data_corrupted(format!(
                        "HNSW definition {} references missing column {}",
                        definition.definition_id, column_id
                    ))
                })?;
            let LogicalType::Array(inner, dim) = &column.logical_type else {
                return Err(paro_error::data_corrupted(format!(
                    "HNSW definition {} column {} must be VECTOR(Float, N), got {:?}",
                    definition.definition_id, column_id, column.logical_type
                )));
            };
            if !matches!(inner.as_ref(), LogicalType::Float) {
                return Err(paro_error::data_corrupted(format!(
                    "HNSW definition {} column {} must be VECTOR(Float, N), got {:?}",
                    definition.definition_id, column_id, column.logical_type
                )));
            }
            let provider = definition.hnsw_provider_config()?;
            if provider.dimension as usize != *dim {
                return Err(paro_error::data_corrupted(format!(
                    "HNSW definition {} dimension {} does not match column {} dimension {}",
                    definition.definition_id, provider.dimension, column_id, dim
                )));
            }
            let build_contract = provider.build_contract();
            build_contract.validate()?;
            indexed_columns.push(HnswIndexedColumn {
                column_id: *column_id,
                dim: *dim,
                build_contract,
            });
        }
        indexed_columns.sort_by_key(|column| column.column_id);
        Ok(indexed_columns)
    }

    fn collect_old_indexes(
        input_rowsets: &[RowsetSharedPtr],
        indexed_col: HnswIndexedColumn,
    ) -> Result<Vec<Arc<HnswIndex>>> {
        let mut candidates = Vec::new();
        for rowset in input_rowsets {
            rowset.load()?;
            for segment in rowset.segments() {
                let Some(index) = segment.open_hnsw_index(indexed_col.column_id)? else {
                    continue;
                };
                if index.vector_storage.vector_dim() != indexed_col.dim {
                    continue;
                }
                if index.graph.links.num_points() == 0 {
                    continue;
                }
                if index.build_contract != indexed_col.build_contract {
                    continue;
                }
                candidates.push(index);
            }
        }
        Ok(candidates)
    }

    fn rebuild_segment_indexes(
        segment: &Segment,
        indexed_columns: &[HnswIndexedColumn],
        old_indexes_by_col: &HashMap<u32, Vec<Arc<HnswIndex>>>,
    ) -> Result<()> {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(segment.file_path())
            .map_err(|err| {
                paro_error::io_error(format!(
                    "open segment file {} for compaction HNSW rebuild: {}",
                    segment.file_path().display(),
                    err
                ))
            })?;

        let mut footer = Self::read_segment_footer(&mut file)?;
        let mut rewritten = false;

        for indexed_col in indexed_columns {
            let Some(meta_idx) = footer
                .column_metas
                .iter()
                .position(|meta| meta.column_id == indexed_col.column_id)
            else {
                continue;
            };
            let col_meta = footer.column_metas[meta_idx].clone();
            if col_meta.num_rows == 0 {
                continue;
            }

            let output_storage: Arc<dyn VectorStorage> =
                open_plain_vector_column(segment.file_path(), &col_meta, indexed_col.dim)?;

            let old_candidates = old_indexes_by_col
                .get(&indexed_col.column_id)
                .map(Vec::as_slice)
                .unwrap_or(&[]);
            let rebuilt_index = Self::build_segment_index(
                output_storage,
                *indexed_col,
                old_candidates,
                segment.file_path().parent().ok_or_else(|| {
                    paro_error::internal("compaction segment path has no parent directory")
                })?,
            )?;
            let ptr = Self::write_hnsw_page(&mut file, &rebuilt_index)?;

            let target = &mut footer.column_metas[meta_idx];
            let previous_size = target
                .hnsw_index_pointer
                .map(|p| p.size as u64)
                .unwrap_or_default();
            target.hnsw_index_pointer = Some(ptr);
            target.hnsw_index_statistics = Some(HnswIndexStatistics::collect(&rebuilt_index)?);
            target.total_mem_footprint = target
                .total_mem_footprint
                .saturating_sub(previous_size)
                .saturating_add(ptr.size as u64);
            rewritten = true;
        }

        if rewritten {
            file.seek(SeekFrom::End(0)).map_err(paro_error::io)?;
            let footer_bytes = footer.serialize();
            file.write_all(&footer_bytes).map_err(paro_error::io)?;
            file.flush().map_err(paro_error::io)?;
        }

        Ok(())
    }

    fn build_segment_index(
        output_storage: Arc<dyn VectorStorage>,
        indexed_col: HnswIndexedColumn,
        old_candidates: &[Arc<HnswIndex>],
        workspace_dir: &std::path::Path,
    ) -> Result<HnswIndex> {
        if let Some(index) = Self::build_index_with_healer(
            output_storage.clone(),
            indexed_col,
            old_candidates,
            workspace_dir,
        )? {
            return Ok(index);
        }

        HnswIndex::try_build_in_workspace(output_storage, indexed_col.build_contract, workspace_dir)
    }

    fn build_index_with_healer(
        output_storage: Arc<dyn VectorStorage>,
        indexed_col: HnswIndexedColumn,
        old_candidates: &[Arc<HnswIndex>],
        workspace_dir: &std::path::Path,
    ) -> Result<Option<HnswIndex>> {
        let output_storage =
            IndexedVectorStorage::prepare(output_storage, indexed_col.build_contract.distance);
        let output_storage = prepare_build_vector_storage(
            output_storage,
            indexed_col.build_contract.vector_encoding,
            indexed_col.build_contract.build_seed,
            Some(workspace_dir),
            None,
        )?;
        let Some((best_old_index, signature_overlap)) = Self::select_best_old_index(
            output_storage.as_ref(),
            old_candidates,
            indexed_col.build_contract,
        ) else {
            return Ok(None);
        };
        let overlap_ratio = signature_overlap as f64 / output_storage.num_vectors().max(1) as f64;
        if overlap_ratio < MIN_HEALER_OVERLAP_RATIO {
            return Ok(None);
        }

        let (old_to_new, new_to_old, matched) = Self::build_old_new_mapping(
            best_old_index.vector_storage.as_ref(),
            output_storage.as_ref(),
        );
        if matched == 0 {
            return Ok(None);
        }

        let build_contract = indexed_col.build_contract;
        let mut builder = GraphLayersBuilder::new_from_contract(
            output_storage.num_vectors(),
            &build_contract,
            true,
        );

        for (new_id, old_id_opt) in new_to_old.iter().enumerate() {
            let point_id = new_id as PointOffset;
            let level = match old_id_opt {
                Some(old_id) => best_old_index.graph.links.point_level(*old_id)?,
                None => builder.random_layer_for_point(point_id),
            };
            builder.set_levels(point_id, level);
        }

        let mut healer = GraphLayersHealer::new(
            &best_old_index.graph,
            &old_to_new,
            indexed_col.build_contract.ef_construct as usize,
        )?;
        healer.heal(
            best_old_index.vector_storage.as_ref(),
            indexed_col.build_contract.distance,
        )?;
        healer.save_into_builder(&builder);

        for (new_id, old_id_opt) in new_to_old.iter().enumerate() {
            if old_id_opt.is_some() {
                continue;
            }
            let point_id = new_id as PointOffset;
            builder.insert_single_point(
                point_id,
                output_storage.as_ref(),
                indexed_col.build_contract.distance,
            )?;
        }

        let (links, entry_points) = builder.into_graph_data()?;
        let graph = GraphLayers::new(links, entry_points, (&build_contract).into());
        Ok(Some(HnswIndex::try_new(
            build_contract,
            graph,
            output_storage,
        )?))
    }

    fn select_best_old_index(
        output_storage: &dyn VectorStorage,
        old_candidates: &[Arc<HnswIndex>],
        expected_contract: HnswBuildContract,
    ) -> Option<(Arc<HnswIndex>, usize)> {
        if old_candidates.is_empty() || output_storage.num_vectors() == 0 {
            return None;
        }

        let output_counts = Self::build_signature_count_map(output_storage);
        let mut best: Option<(Arc<HnswIndex>, usize)> = None;
        for candidate in old_candidates
            .iter()
            .filter(|candidate| candidate.build_contract == expected_contract)
        {
            let overlap =
                Self::count_signature_overlap(candidate.vector_storage.as_ref(), &output_counts);
            match best {
                Some((_, best_overlap)) if overlap <= best_overlap => {}
                _ => {
                    best = Some((candidate.clone(), overlap));
                }
            }
        }
        best.filter(|(_, overlap)| *overlap > 0)
    }

    fn build_signature_count_map(storage: &dyn VectorStorage) -> HashMap<Vec<u8>, usize> {
        let mut counts = HashMap::new();
        for idx in 0..storage.num_vectors() {
            let key = Self::vector_signature(storage.get_vector(idx as PointOffset));
            *counts.entry(key).or_insert(0) += 1;
        }
        counts
    }

    fn count_signature_overlap(
        old_storage: &dyn VectorStorage,
        output_counts: &HashMap<Vec<u8>, usize>,
    ) -> usize {
        let mut remaining = output_counts.clone();
        let mut matched = 0usize;
        for old_id in 0..old_storage.num_vectors() {
            let key = Self::vector_signature(old_storage.get_vector(old_id as PointOffset));
            if let Some(count) = remaining.get_mut(&key) {
                if *count > 0 {
                    *count -= 1;
                    matched += 1;
                }
            }
        }
        matched
    }

    fn build_old_new_mapping(
        old_storage: &dyn VectorStorage,
        output_storage: &dyn VectorStorage,
    ) -> (Vec<Option<PointOffset>>, Vec<Option<PointOffset>>, usize) {
        let mut output_buckets: HashMap<Vec<u8>, Vec<PointOffset>> = HashMap::new();
        for new_id in 0..output_storage.num_vectors() {
            let new_id = new_id as PointOffset;
            let key = Self::vector_signature(output_storage.get_vector(new_id));
            output_buckets.entry(key).or_default().push(new_id);
        }

        // Keep mapping deterministic: pop smallest new id first.
        output_buckets
            .values_mut()
            .for_each(|bucket| bucket.reverse());

        let mut old_to_new = vec![None; old_storage.num_vectors()];
        let mut new_to_old = vec![None; output_storage.num_vectors()];
        let mut matched = 0usize;

        for old_id in 0..old_storage.num_vectors() {
            let old_id = old_id as PointOffset;
            let key = Self::vector_signature(old_storage.get_vector(old_id));
            let Some(bucket) = output_buckets.get_mut(&key) else {
                continue;
            };
            let Some(new_id) = bucket.pop() else {
                continue;
            };

            old_to_new[old_id as usize] = Some(new_id);
            new_to_old[new_id as usize] = Some(old_id);
            matched += 1;
        }

        (old_to_new, new_to_old, matched)
    }

    fn vector_signature(vector: &[f32]) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(std::mem::size_of_val(vector));
        for value in vector {
            bytes.extend_from_slice(&value.to_bits().to_le_bytes());
        }
        bytes
    }

    fn read_segment_footer(file: &mut File) -> Result<SegmentFooter> {
        let file_size = file.metadata().map_err(paro_error::io)?.len();
        if file_size < 4 {
            return Err(paro_error::data_corrupted(format!(
                "segment file too small for footer: {} bytes",
                file_size
            )));
        }

        file.seek(SeekFrom::End(-4)).map_err(paro_error::io)?;
        let mut footer_size_buf = [0u8; 4];
        file.read_exact(&mut footer_size_buf)
            .map_err(paro_error::io)?;
        let footer_size = u32::from_le_bytes(footer_size_buf) as u64;

        if footer_size < 8 || footer_size > file_size {
            return Err(paro_error::data_corrupted(format!(
                "invalid segment footer size: {} (file size {})",
                footer_size, file_size
            )));
        }

        file.seek(SeekFrom::End(-(footer_size as i64)))
            .map_err(paro_error::io)?;
        let mut footer_bytes = vec![0u8; footer_size as usize - 4];
        file.read_exact(&mut footer_bytes).map_err(paro_error::io)?;
        SegmentFooter::deserialize(&footer_bytes)
    }

    fn write_hnsw_page(file: &mut File, index: &HnswIndex) -> Result<PagePointer> {
        let index_data = index.serialize()?;
        let footer = PageFooter::Index(IndexPageFooter {
            num_entries: index.graph.links.num_points() as u32,
            page_type: IndexPageType::Leaf,
        });

        file.seek(SeekFrom::End(0)).map_err(paro_error::io)?;
        PageIO::write_mmap_page(file, &index_data, &footer, HNSW_ARTIFACT_ALIGNMENT)
    }

    fn validate_compaction_indexes(
        rowset: &RowsetSharedPtr,
        indexed_columns: &[HnswIndexedColumn],
    ) -> Result<()> {
        for segment in rowset.segments() {
            // Re-open from disk to validate persisted footer/index pointers.
            let persisted = Segment::open(
                segment.segment_id(),
                segment.file_path(),
                rowset.schema().clone(),
                SegmentOptions::default(),
                rowset.tablet_id(),
                rowset.rowset_id(),
                rowset.rowset_gen(),
            )?;

            let segment_rows = persisted.num_rows() as usize;
            for indexed_col in indexed_columns {
                let col_meta = persisted
                    .get_column_meta(indexed_col.column_id)
                    .ok_or_else(|| {
                        paro_error::data_corrupted(format!(
                            "Missing indexed column meta after compaction: segment={} column={}",
                            persisted.segment_id(),
                            indexed_col.column_id
                        ))
                    })?;
                if col_meta.num_rows == 0 {
                    continue;
                }

                let index = persisted
                    .open_hnsw_index(indexed_col.column_id)?
                    .ok_or_else(|| {
                        paro_error::data_corrupted(format!(
                            "Missing HNSW index after compaction: segment={} column={}",
                            persisted.segment_id(),
                            indexed_col.column_id
                        ))
                    })?;

                let points = index.graph.links.num_points();
                if points != segment_rows {
                    return Err(paro_error::data_corrupted(format!(
                        "HNSW point count mismatch after compaction: segment={} column={} points={} rows={}",
                        persisted.segment_id(),
                        indexed_col.column_id,
                        points,
                        segment_rows
                    )));
                }
            }
        }
        Ok(())
    }
}

/// Rowset compaction may only rebuild HNSW inside the rowset when the
/// definition promises synchronous physical freshness. Opportunistic and
/// bounded-lag definitions are owned by the search generation: rebuilding an
/// inline graph here creates a second artifact lifecycle, duplicates graph
/// construction, and multiplies query-time beam work after every table
/// compaction. Those definitions deliberately expose the compacted rowset as
/// an exact tail until generation maintenance publishes its replacement.
fn compaction_requires_inline_hnsw(definition: &SearchIndexDefinition) -> bool {
    definition.kind == SearchIndexKind::Hnsw
        && definition.freshness_policy == SearchFreshnessPolicy::Required
}

impl CompactionIndexRebuilder for HnswIndexRebuilder {
    fn name(&self) -> &'static str {
        "HNSW"
    }

    fn is_applicable(
        &self,
        generation_context: &CompactionGenerationContext,
        _tablet: &Tablet,
        _rowset: &RowsetSharedPtr,
        _plan: &CompactionPlan,
    ) -> bool {
        generation_context
            .search_definitions
            .iter()
            .any(compaction_requires_inline_hnsw)
    }

    fn rebuild(
        &self,
        generation_context: &CompactionGenerationContext,
        tablet: &Tablet,
        rowset: &RowsetSharedPtr,
        plan: &CompactionPlan,
    ) -> Result<()> {
        rowset.load()?;
        let indexed_columns = Self::collect_indexed_columns(generation_context, tablet)?;
        if indexed_columns.is_empty() {
            return Ok(());
        }

        let mut old_indexes_by_col: HashMap<u32, Vec<Arc<HnswIndex>>> = HashMap::new();
        let input_rowsets = plan.input_rowset_ptrs();
        for indexed_col in &indexed_columns {
            old_indexes_by_col.insert(
                indexed_col.column_id,
                Self::collect_old_indexes(&input_rowsets, *indexed_col)?,
            );
        }

        for segment in rowset.segments() {
            Self::rebuild_segment_indexes(&segment, &indexed_columns, &old_indexes_by_col)?;
        }

        // Refresh rowset segments so in-memory readers see newly appended footers.
        rowset.reload()?;
        Self::validate_compaction_indexes(rowset, &indexed_columns)
    }
}

pub fn register_hnsw_rebuilder() -> Result<()> {
    crate::compaction::execution::index_rebuild::register_compaction_index_rebuilder(Arc::new(
        HnswIndexRebuilder::new(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::hnsw::InMemoryVectorStorage;
    use crate::rowset::segment::{ColumnData, SegmentWriter, SegmentWriterOptions};
    use crate::tablet::tablet_schema::{KeysType, TabletColumn, TabletSchema};
    use std::path::Path;
    use tempfile::TempDir;

    fn definition(
        kind: SearchIndexKind,
        freshness_policy: SearchFreshnessPolicy,
    ) -> SearchIndexDefinition {
        SearchIndexDefinition {
            definition_id: 7,
            table_id: 11,
            name: "test_index".to_string(),
            kind,
            column_ids: vec![1],
            expression: None,
            provider_config: serde_json::Value::Null,
            freshness_policy,
            config_fingerprint: 13,
        }
    }

    #[test]
    fn rowset_compaction_rebuilds_only_required_hnsw_inline_artifacts() {
        assert!(compaction_requires_inline_hnsw(&definition(
            SearchIndexKind::Hnsw,
            SearchFreshnessPolicy::Required,
        )));
        assert!(!compaction_requires_inline_hnsw(&definition(
            SearchIndexKind::Hnsw,
            SearchFreshnessPolicy::Opportunistic,
        )));
        assert!(!compaction_requires_inline_hnsw(&definition(
            SearchIndexKind::Hnsw,
            SearchFreshnessPolicy::BoundedLag {
                max_tail_rows: 4_096,
                max_lag_millis: 1_000,
            },
        )));
        assert!(!compaction_requires_inline_hnsw(&definition(
            SearchIndexKind::FullText,
            SearchFreshnessPolicy::Required,
        )));
    }

    fn storage(vectors: &[Vec<f32>]) -> InMemoryVectorStorage {
        let dim = vectors[0].len();
        let mut flat = Vec::with_capacity(vectors.len() * dim);
        for v in vectors {
            flat.extend_from_slice(v);
        }
        InMemoryVectorStorage::new(flat, dim)
    }

    #[test]
    fn old_new_mapping_handles_partial_overlap() {
        let old = storage(&[vec![1.0, 1.0], vec![2.0, 2.0], vec![3.0, 3.0]]);
        let new = storage(&[vec![2.0, 2.0], vec![4.0, 4.0], vec![1.0, 1.0]]);

        let (old_to_new, new_to_old, matched) =
            HnswIndexRebuilder::build_old_new_mapping(&old, &new);

        assert_eq!(matched, 2);
        assert_eq!(old_to_new[0], Some(2));
        assert_eq!(old_to_new[1], Some(0));
        assert_eq!(old_to_new[2], None);
        assert_eq!(new_to_old[0], Some(1));
        assert_eq!(new_to_old[1], None);
        assert_eq!(new_to_old[2], Some(0));
    }

    #[test]
    fn healer_falls_back_to_full_rebuild_when_overlap_is_too_low() {
        let config = HnswConfig::new(8, 32);
        let distance = DistanceMetric::Euclidean;
        let indexed_col = HnswIndexedColumn {
            column_id: 1,
            dim: 4,
            build_contract: config.try_build_contract(distance).unwrap(),
        };

        let old_vectors = storage(&[
            vec![1.0, 1.0, 1.0, 1.0],
            vec![2.0, 2.0, 2.0, 2.0],
            vec![3.0, 3.0, 3.0, 3.0],
            vec![4.0, 4.0, 4.0, 4.0],
        ]);
        let output_vectors = storage(&[
            vec![1.0, 1.0, 1.0, 1.0],
            vec![9.0, 9.0, 9.0, 9.0],
            vec![8.0, 8.0, 8.0, 8.0],
            vec![7.0, 7.0, 7.0, 7.0],
        ]);

        let old_index = Arc::new(HnswIndex::build(Arc::new(old_vectors), config, distance));
        let workspace = TempDir::new().unwrap();
        let healed = HnswIndexRebuilder::build_index_with_healer(
            Arc::new(output_vectors),
            indexed_col,
            &[old_index],
            workspace.path(),
        )
        .unwrap();
        assert!(
            healed.is_none(),
            "low-overlap compaction should fallback to full rebuild"
        );
    }

    #[test]
    fn healer_ignores_source_artifacts_from_a_different_definition_contract() {
        let config = HnswConfig::new(8, 32);
        let distance = DistanceMetric::Euclidean;
        let mut compact_contract = config.try_build_contract(distance).unwrap();
        compact_contract.vector_encoding =
            crate::index::hnsw::HnswBuildVectorEncoding::symmetric_i16(4).unwrap();
        let indexed_col = HnswIndexedColumn {
            column_id: 1,
            dim: 4,
            build_contract: compact_contract,
        };
        let vectors = vec![
            vec![1.0, 1.0, 1.0, 1.0],
            vec![2.0, 2.0, 2.0, 2.0],
            vec![3.0, 3.0, 3.0, 3.0],
            vec![4.0, 4.0, 4.0, 4.0],
        ];
        let old_index = Arc::new(HnswIndex::build(
            Arc::new(storage(&vectors)),
            config,
            distance,
        ));
        let workspace = TempDir::new().unwrap();

        let healed = HnswIndexRebuilder::build_index_with_healer(
            Arc::new(storage(&vectors)),
            indexed_col,
            &[old_index],
            workspace.path(),
        )
        .unwrap();

        assert!(
            healed.is_none(),
            "definition-owned build contracts must not be inferred from heterogeneous inputs"
        );
    }

    fn write_vector_segment(
        schema: Arc<TabletSchema>,
        path: &Path,
        vectors: &[Vec<f32>],
        build_hnsw: bool,
        config: HnswConfig,
        distance: DistanceMetric,
    ) {
        let opts = SegmentWriterOptions::new(0)
            .with_build_hnsw_indexes(build_hnsw)
            .with_hnsw_index(1, config, distance);
        let mut writer = SegmentWriter::create(schema, path, opts).unwrap();

        let mut ids = Vec::with_capacity(vectors.len() * std::mem::size_of::<i64>());
        let mut vec_bytes = Vec::new();
        for (i, vector) in vectors.iter().enumerate() {
            ids.extend_from_slice(&(i as i64).to_le_bytes());
            for value in vector {
                vec_bytes.extend_from_slice(&value.to_le_bytes());
            }
        }

        writer
            .append_chunk(&[
                ColumnData::new(ids, vectors.len() as u32),
                ColumnData::new(vec_bytes, vectors.len() as u32),
            ])
            .unwrap();
        let segment = writer.finalize().unwrap();
        if build_hnsw {
            let pointer = segment
                .get_column_meta(1)
                .and_then(|meta| meta.hnsw_index_pointer)
                .expect("test fixture HNSW page");
            assert_eq!(
                pointer.offset % HNSW_ARTIFACT_ALIGNMENT as u64,
                0,
                "test fixture must publish an aligned HNSW envelope"
            );
        }
    }

    #[test]
    fn rebuild_segment_indexes_materializes_missing_hnsw_page() {
        let temp_dir = TempDir::new().unwrap();
        let old_path = temp_dir.path().join("old.dat");
        let new_path = temp_dir.path().join("new.dat");

        let dim = 4usize;
        let columns = vec![
            TabletColumn::new(0, "id", LogicalType::BigInt),
            TabletColumn::new(
                1,
                "vec",
                LogicalType::Array(Box::new(LogicalType::Float), dim),
            ),
        ];
        let schema = Arc::new(TabletSchema::new(1, columns, KeysType::DuplicateKeys).unwrap());
        let config = HnswConfig::new(8, 32);
        let distance = DistanceMetric::Euclidean;

        let old_vectors = vec![
            vec![1.0, 1.0, 1.0, 1.0],
            vec![2.0, 2.0, 2.0, 2.0],
            vec![3.0, 3.0, 3.0, 3.0],
            vec![4.0, 4.0, 4.0, 4.0],
        ];
        write_vector_segment(
            schema.clone(),
            &old_path,
            &old_vectors,
            true,
            config,
            distance,
        );

        let new_vectors = vec![
            vec![1.0, 1.0, 1.0, 1.0],
            vec![2.0, 2.0, 2.0, 2.0],
            vec![3.0, 3.0, 3.0, 3.0],
            vec![4.0, 4.0, 4.0, 4.0],
            vec![9.0, 9.0, 9.0, 9.0],
        ];
        write_vector_segment(
            schema.clone(),
            &new_path,
            &new_vectors,
            false,
            config,
            distance,
        );

        let old_segment = Segment::open(
            0,
            &old_path,
            schema.clone(),
            SegmentOptions::default(),
            0,
            0,
            0,
        )
        .unwrap();
        let old_index = old_segment
            .open_hnsw_index(1)
            .unwrap()
            .expect("old hnsw index");

        let new_segment = Segment::open(
            0,
            &new_path,
            schema.clone(),
            SegmentOptions::default(),
            0,
            1,
            0,
        )
        .unwrap();
        assert!(
            new_segment.open_hnsw_index(1).unwrap().is_none(),
            "new segment should not have HNSW index before compaction rebuild"
        );

        let indexed_col = HnswIndexedColumn {
            column_id: 1,
            dim,
            build_contract: config.try_build_contract(distance).unwrap(),
        };
        let mut old_indexes_by_col = HashMap::new();
        old_indexes_by_col.insert(1, vec![old_index]);
        HnswIndexRebuilder::rebuild_segment_indexes(
            &new_segment,
            &[indexed_col],
            &old_indexes_by_col,
        )
        .unwrap();

        let rebuilt =
            Segment::open(0, &new_path, schema, SegmentOptions::default(), 0, 1, 0).unwrap();
        let rebuilt_index = rebuilt
            .open_hnsw_index(1)
            .unwrap()
            .expect("rebuilt segment should contain HNSW index");
        assert_eq!(rebuilt_index.graph.links.num_points(), new_vectors.len());
    }
}
