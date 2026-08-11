// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::collections::HashSet;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use paro_common::allocator::MemoryTag;
use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::vector::VECTOR_SIZE;
use paro_function::scalar::FunctionExecContext;

use paro_storage::index::{collect_predicate_columns, PredicateTree};
use paro_storage::rowset::{RowsetSharedPtr, SegmentOptions, SegmentSharedPtr};
use paro_storage::table::segment_reorderer::{reorder_segments, SegmentOrderOptions};
use paro_storage::tablet::{ColumnProjection, TabletReaderParams};
use paro_storage::transaction::overlay_reader::TxnOverlayReader;

use crate::physical::specs::{RowsetColumnProjection, RowsetScanSpec};
use crate::pipeline::graph::RowsetSourceSpec;
use crate::runtime::breaker::{HandleRef, JoinBuildHandle};
use crate::runtime::context::{OperatorCallContext, PipelineInitContext};
use crate::runtime::source::SourcePoll;
use crate::runtime::state::{
    RowsetScanMorsel, RowsetSourceGlobal, RowsetSourceLocal, SourceGlobal, SourceLocal,
};

/// Bounds for scheduler-aware scan morsels.
///
/// Large scans retain coarse morsels so reader construction stays amortized.
/// Smaller scans are split just far enough to occupy the query's worker set;
/// this matters for single-segment dimension tables feeding blocking joins.
const MIN_ROWSET_MORSEL_ROWS: u64 = VECTOR_SIZE as u64;
const MAX_ROWSET_MORSEL_ROWS: u64 = 256 * 1024;

#[derive(Debug, Clone)]
pub struct RowsetSourceExec {
    pub desc: RowsetSourceDesc,
}

#[derive(Debug, Clone)]
pub struct RowsetSourceDesc {
    pub table_index: usize,
    pub table: Arc<paro_catalog::entry::TableCatalogEntry>,
    pub column_projection: RowsetColumnProjection,
    pub emit_row_id: bool,
    pub returned_types: Box<[paro_common::types::LogicalType]>,
    pub predicate: Option<PredicateTree>,
    pub late_materialize: bool,
    pub scan_access_cost: paro_storage::rowset::scan_cost::ScanAccessCostModel,
    pub scan_order: Option<SegmentOrderOptions>,
    pub dynamic_runtime_filters: Box<[RowsetDynamicRuntimeFilterDesc]>,
}

#[derive(Debug, Clone)]
pub struct RowsetDynamicRuntimeFilterDesc {
    pub handle: HandleRef<JoinBuildHandle>,
    pub build_key_index: usize,
    pub probe_column_id: u32,
}

impl RowsetSourceDesc {
    pub fn from_plan_spec(spec: &RowsetScanSpec) -> Self {
        Self {
            table_index: spec.table_index,
            table: spec.table.clone(),
            column_projection: spec.column_projection.clone(),
            emit_row_id: spec.emit_row_id,
            returned_types: spec.returned_types.clone(),
            predicate: spec.predicate.clone(),
            late_materialize: spec.late_materialize,
            scan_access_cost: spec.scan_access_cost,
            scan_order: spec.scan_order.clone(),
            dynamic_runtime_filters: Vec::new().into_boxed_slice(),
        }
    }

    pub fn from_source_spec(spec: &RowsetSourceSpec) -> Self {
        let mut desc = Self::from_plan_spec(&spec.scan);
        desc.dynamic_runtime_filters = spec
            .dynamic_runtime_filters
            .iter()
            .map(|filter| RowsetDynamicRuntimeFilterDesc {
                handle: HandleRef::new(filter.handle),
                build_key_index: filter.build_key_index,
                probe_column_id: filter.probe_column_id,
            })
            .collect::<Vec<_>>()
            .into_boxed_slice();
        desc
    }
}

impl RowsetSourceExec {
    pub(crate) fn create_global(&self, ctx: &mut PipelineInitContext) -> Result<SourceGlobal> {
        let table = self
            .desc
            .table
            .get_storage()
            .cloned()
            .ok_or_else(|| paro_error::internal("rowset scan table has no storage handle"))?;
        let storage_snapshot = table.storage_snapshot(
            ctx.query.transaction.read_ts(),
            ctx.query.transaction.read_snapshot().lease(),
        )?;
        let overlay = TxnOverlayReader::for_tablet(&table.tablet(), &ctx.query.transaction)?;
        let segment_options = SegmentOptions::default()
            .with_page_cache(ctx.query.session.page_cache().clone())
            .with_cache_decoded(true)
            .with_scan_access_cost(self.desc.scan_access_cost);
        let mut segments = storage_snapshot.segments_with_options(segment_options.clone())?;
        if let Some(overlay) = &overlay {
            let visible_rowsets = segments
                .iter()
                .map(|(rowset, _)| rowset.rowset_id())
                .collect::<HashSet<_>>();
            segments.extend(
                overlay
                    .segments_with_options(segment_options)?
                    .into_iter()
                    .filter(|(rowset, _)| !visible_rowsets.contains(&rowset.rowset_id())),
            );
        }
        if let Some(order) = self.desc.scan_order.as_ref() {
            reorder_segments(&mut segments, order);
        }
        let overlay_delete_vectors = overlay.as_ref().and_then(TxnOverlayReader::delete_vectors);
        let column_projection = match &self.desc.column_projection {
            RowsetColumnProjection::All => {
                ColumnProjection::new((0..table.types().len()).collect())
            }
            RowsetColumnProjection::Columns(columns) => ColumnProjection::new(columns.to_vec()),
        };
        let predicate = self.effective_predicate(ctx)?;
        let predicate_columns = predicate
            .as_ref()
            .map(collect_predicate_columns)
            .unwrap_or_default()
            .into_boxed_slice();
        let morsels = build_scan_morsels(&segments, ctx.query.session.number_of_threads().max(1));

        Ok(SourceGlobal::Rowset(Arc::new(RowsetSourceGlobal {
            table_index: self.desc.table_index,
            table,
            storage_snapshot,
            segments: segments.into_boxed_slice(),
            morsels,
            next_morsel: Default::default(),
            column_projection,
            overlay_delete_vectors,
            predicate,
            predicate_columns,
        })))
    }

    pub(crate) fn create_local(
        &self,
        _ctx: &mut PipelineInitContext,
        global: &SourceGlobal,
    ) -> Result<SourceLocal> {
        global.rowset()?;
        Ok(SourceLocal::Rowset(RowsetSourceLocal::default()))
    }

    pub(crate) fn poll_next(
        &self,
        ctx: &mut OperatorCallContext,
        global: &SourceGlobal,
        local: &mut SourceLocal,
        output: &mut Chunk,
    ) -> Result<SourcePoll> {
        let global = global.rowset()?;
        let local = local.rowset_mut()?;

        loop {
            ctx.cancel.check()?;
            if local.reader.is_none() {
                let morsel_idx = global.next_morsel.fetch_add(1, Ordering::AcqRel);
                let Some(morsel) = global.morsels.get(morsel_idx) else {
                    return Ok(SourcePoll::Finished);
                };
                let (rowset, segment) =
                    global.segments.get(morsel.segment_idx).ok_or_else(|| {
                        paro_error::internal("rowset scan morsel references an invalid segment")
                    })?;

                let mut params =
                    TabletReaderParams::with_version(global.storage_snapshot.visible_version())
                        .with_projection(global.column_projection.clone())
                        .with_emit_row_id(self.desc.emit_row_id)
                        .with_segment_handle(Arc::clone(segment))
                        .with_segment_ordinal_range(morsel.start_ordinal, morsel.end_ordinal);
                if let Some(predicate) = &global.predicate {
                    params = params.with_predicates(predicate.clone());
                    if self.desc.late_materialize && !global.predicate_columns.is_empty() {
                        params = params.with_late_materialize(global.predicate_columns.to_vec());
                    }
                }
                if let Some(delete_vectors) = &global.overlay_delete_vectors {
                    params = params.with_overlay_delete_vectors(Arc::clone(delete_vectors));
                }
                let mut reader = global.table.create_reader_with_allocator(
                    params,
                    ctx.query.allocator(MemoryTag::ColumnData),
                )?;
                reader.prepare_with_pinned_rowsets(vec![rowset.clone()])?;
                local.reader = Some(reader);
            }

            let reader = local.reader.as_mut().expect("rowset reader initialized");
            match reader.get_next_chunk()? {
                Some(mut chunk) => {
                    // The scratch chunk is only an ownership slot here; move
                    // the reader-owned vector array into it without cloning.
                    output.move_from(&mut chunk);
                    return Ok(SourcePoll::Output);
                }
                None => {
                    local.reader = None;
                }
            }
        }
    }

    fn effective_predicate(&self, ctx: &PipelineInitContext<'_>) -> Result<Option<PredicateTree>> {
        let mut predicates = Vec::new();
        if let Some(predicate) = &self.desc.predicate {
            predicates.push(predicate.clone());
        }
        for filter in &self.desc.dynamic_runtime_filters {
            let handle = ctx.handles.get(filter.handle)?;
            if !handle.runtime_filter_ready() {
                return Err(paro_error::internal(format!(
                    "hash join runtime filter {} was not published before rowset scan of {}",
                    filter.handle.id().index(),
                    self.desc.table.name()
                )));
            }
            if let Some(predicate) =
                handle.runtime_filter_predicate(filter.build_key_index, filter.probe_column_id)
            {
                predicates.push(predicate);
            }
        }
        Ok(combine_predicates(predicates))
    }
}

fn build_scan_morsels(
    segments: &[(RowsetSharedPtr, SegmentSharedPtr)],
    parallelism: usize,
) -> Box<[RowsetScanMorsel]> {
    let total_rows = segments.iter().fold(0u64, |total, (_, segment)| {
        total.saturating_add(segment.num_rows())
    });
    let morsel_rows = rowset_morsel_rows(total_rows, parallelism);
    segments
        .iter()
        .enumerate()
        .flat_map(|(segment_idx, (_, segment))| {
            let row_count = segment.num_rows();
            (0..row_count)
                .step_by(morsel_rows as usize)
                .map(move |start_ordinal| RowsetScanMorsel {
                    segment_idx,
                    start_ordinal,
                    end_ordinal: row_count.min(start_ordinal + morsel_rows),
                })
        })
        .collect::<Vec<_>>()
        .into_boxed_slice()
}

fn rowset_morsel_rows(total_rows: u64, parallelism: usize) -> u64 {
    if parallelism <= 1 {
        // Morsels are scheduling units, not storage batches. With only one
        // worker there is nobody to steal trailing work, so splitting a
        // segment merely rebuilds its reader and reopens its columns. Keep one
        // morsel per segment while respecting step_by's platform-sized input.
        let max_step = u64::try_from(usize::MAX).unwrap_or(u64::MAX);
        return total_rows.max(MIN_ROWSET_MORSEL_ROWS).min(max_step);
    }

    let parallelism = u64::try_from(parallelism).unwrap_or(u64::MAX).max(1);
    total_rows
        .div_ceil(parallelism)
        .clamp(MIN_ROWSET_MORSEL_ROWS, MAX_ROWSET_MORSEL_ROWS)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn morsels_expose_workers_without_fragmenting_large_scans() {
        assert_eq!(rowset_morsel_rows(25, 4), MIN_ROWSET_MORSEL_ROWS);
        assert_eq!(rowset_morsel_rows(10_000, 4), MIN_ROWSET_MORSEL_ROWS);
        assert_eq!(rowset_morsel_rows(200_000, 4), 50_000);
        assert_eq!(rowset_morsel_rows(800_000, 4), 200_000);
        assert_eq!(rowset_morsel_rows(6_000_000, 4), MAX_ROWSET_MORSEL_ROWS);
    }

    #[test]
    fn morsel_policy_handles_empty_and_single_thread_scans() {
        assert_eq!(rowset_morsel_rows(0, 0), MIN_ROWSET_MORSEL_ROWS);
        assert_eq!(rowset_morsel_rows(200_000, 1), 200_000);
        assert_eq!(rowset_morsel_rows(6_000_000, 1), 6_000_000);
        assert_eq!(
            rowset_morsel_rows(u64::MAX, usize::MAX),
            MIN_ROWSET_MORSEL_ROWS
        );
    }
}

fn combine_predicates(predicates: Vec<PredicateTree>) -> Option<PredicateTree> {
    match predicates.len() {
        0 => None,
        1 => predicates.into_iter().next(),
        _ => Some(PredicateTree::And(predicates)),
    }
}
