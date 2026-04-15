//! Physical rowset scan operator.
//!
//! MVP implementation that reads Tablet data via TabletReader using
//! the transaction visible version.

use std::any::Any;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

use crate::execution_context::ExecutionContext;
use crate::explain::explain_node::format_predicate_tree;
use crate::operator::scan::{column_pruning, late_materialize, memory_budget};
use crate::operator::state::{
    GlobalSourceState, LocalSourceState, OperatorSourceInput, ProgressData,
};
use crate::operator::PhysicalOperator;
use crate::operator_type::PhysicalOperatorType;
use crate::result_type::SourceResultType;
use paro_common::allocator::MemoryTag;
use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::types::LogicalType;
use paro_storage::buffer::{PageCache, Prefetcher, TemporaryMemoryState};
use paro_storage::compression::ParallelDecompressor;
use paro_storage::index::PredicateTree;
use paro_storage::rowset::{RowsetSharedPtr, SegmentOptions, SegmentSharedPtr};
use paro_storage::table::table_handle::TableHandle;
use paro_storage::tablet::{ColumnProjection, TabletReader, TabletReaderParams};

#[derive(Debug)]
pub struct RowsetScanBindData {
    pub(crate) table_data: Arc<TableHandle>,
    pub(crate) column_ids: Vec<usize>,
    pub(crate) output_types: Vec<LogicalType>,
    pub(crate) predicate_tree: Option<PredicateTree>,
    pub(crate) emit_row_id: bool,
    pub(crate) relation_name: Option<String>,
    pub(crate) relation_alias: Option<String>,
}

impl RowsetScanBindData {
    pub fn from_table_data(table_data: Arc<TableHandle>) -> Self {
        let output_types = table_data.types().to_vec();
        Self {
            table_data,
            column_ids: Vec::new(),
            output_types,
            predicate_tree: None,
            emit_row_id: false,
            relation_name: None,
            relation_alias: None,
        }
    }

    pub fn from_table_data_with_projection(
        table_data: Arc<TableHandle>,
        column_ids: Vec<usize>,
    ) -> Self {
        let all_types = table_data.types();
        let output_types = column_ids
            .iter()
            .filter_map(|&idx| all_types.get(idx).cloned())
            .collect();
        Self {
            table_data,
            column_ids,
            output_types,
            predicate_tree: None,
            emit_row_id: false,
            relation_name: None,
            relation_alias: None,
        }
    }

    pub fn with_predicate(mut self, predicate_tree: PredicateTree) -> Self {
        self.predicate_tree = Some(predicate_tree);
        self
    }

    pub fn with_emit_row_id(mut self, emit_row_id: bool) -> Self {
        self.emit_row_id = emit_row_id;
        self
    }

    pub fn with_output_types(mut self, output_types: Vec<LogicalType>) -> Self {
        self.output_types = output_types;
        self
    }

    pub fn with_relation(
        mut self,
        relation_name: Option<String>,
        relation_alias: Option<String>,
    ) -> Self {
        self.relation_name = relation_name;
        self.relation_alias = relation_alias;
        self
    }
}

#[derive(Debug)]
pub struct PhysicalRowsetScan {
    bind_data: RowsetScanBindData,
}

impl PhysicalRowsetScan {
    pub fn new(bind_data: RowsetScanBindData) -> Self {
        Self { bind_data }
    }

    pub fn projected_column_names(&self) -> Vec<String> {
        let tablet = self.bind_data.table_data.tablet();
        if let Some(schema) = tablet.schema() {
            if self.bind_data.column_ids.is_empty() {
                return schema
                    .columns()
                    .iter()
                    .map(|col| col.name.clone())
                    .collect();
            }
            return self
                .bind_data
                .column_ids
                .iter()
                .map(|&idx| {
                    schema
                        .column(idx)
                        .map(|col| col.name.clone())
                        .unwrap_or_else(|| format!("col_{idx}"))
                })
                .collect();
        }
        if self.bind_data.column_ids.is_empty() {
            (0..self.bind_data.output_types.len())
                .map(|idx| format!("col_{idx}"))
                .collect()
        } else {
            self.bind_data
                .column_ids
                .iter()
                .map(|idx| format!("col_{idx}"))
                .collect()
        }
    }

    pub fn predicate_tree(&self) -> Option<&PredicateTree> {
        self.bind_data.predicate_tree.as_ref()
    }
}

#[derive(Debug, Default)]
struct RowsetScanLocalState {
    reader: Option<TabletReader>,
    current_segment_total_rows: u64,
    current_segment_output_rows: u64,
}

impl LocalSourceState for RowsetScanLocalState {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

#[derive(Debug)]
struct RowsetScanGlobalState {
    segments: Vec<(RowsetSharedPtr, SegmentSharedPtr)>,
    total_rows: u64,
    rows_scanned: AtomicU64,
    next_segment: AtomicUsize,
    max_scan_threads: usize,
    column_projection: ColumnProjection,
    predicate_tree: Option<PredicateTree>,
    segment_options: Option<SegmentOptions>,
    prefetcher: Option<Arc<Prefetcher>>,
    late_materialize_plan: Option<late_materialize::LateMaterializePlan>,
    late_materialize_state: Option<Arc<TemporaryMemoryState>>,
    scan_memory_state: Arc<TemporaryMemoryState>,
    scan_budget_config: memory_budget::ScanMemoryBudgetConfig,
}

impl GlobalSourceState for RowsetScanGlobalState {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn max_threads(&self) -> usize {
        self.max_scan_threads
    }
}

impl Drop for RowsetScanGlobalState {
    fn drop(&mut self) {
        self.scan_memory_state.set_zero();
        if let Some(state) = &self.late_materialize_state {
            state.set_zero();
        }
        if let Some(prefetcher) = &self.prefetcher {
            prefetcher.record_waste();
        }
    }
}

fn progress_from_rows(rows_scanned: u64, total_rows: u64) -> ProgressData {
    if total_rows == 0 {
        return ProgressData::new(1.0, 0);
    }

    let scanned = rows_scanned.min(total_rows);
    let percentage = ((scanned as f64) / (total_rows as f64)).clamp(0.0, 1.0);
    ProgressData::new(percentage, scanned)
}

impl PhysicalOperator for PhysicalRowsetScan {
    fn operator_type(&self) -> PhysicalOperatorType {
        PhysicalOperatorType::RowsetScan
    }

    fn types(&self) -> &[LogicalType] {
        &self.bind_data.output_types
    }

    fn explain_params(&self) -> Vec<String> {
        let mut params = Vec::new();
        let tablet = self.bind_data.table_data.tablet();
        if let Some(relation_name) = &self.bind_data.relation_name {
            let relation = match &self.bind_data.relation_alias {
                Some(alias) => format!("{relation_name} {alias}"),
                None => relation_name.clone(),
            };
            params.push(format!("Relation: {relation}"));
        }

        let column_names: Vec<String> = if let Some(schema) = tablet.schema() {
            if self.bind_data.column_ids.is_empty() {
                schema
                    .columns()
                    .iter()
                    .map(|col| col.name.clone())
                    .collect()
            } else {
                self.bind_data
                    .column_ids
                    .iter()
                    .map(|&idx| {
                        schema
                            .column(idx)
                            .map(|col| col.name.clone())
                            .unwrap_or_else(|| format!("col_{idx}"))
                    })
                    .collect()
            }
        } else if self.bind_data.column_ids.is_empty() {
            (0..self.bind_data.output_types.len())
                .map(|idx| format!("col_{idx}"))
                .collect()
        } else {
            self.bind_data
                .column_ids
                .iter()
                .map(|idx| format!("col_{idx}"))
                .collect()
        };

        if !column_names.is_empty() {
            params.push(format!("Columns: {}", column_names.join(", ")));
        }

        if let Some(predicate) = &self.bind_data.predicate_tree {
            params.push(format!("Filter: {}", format_predicate_tree(predicate)));
        }

        params
    }

    fn is_source(&self) -> bool {
        true
    }

    fn parallel_source(&self) -> bool {
        true
    }

    fn get_global_source_state(
        &self,
        ctx: &ExecutionContext,
        _sink_state: Option<&dyn crate::operator::state::GlobalSinkState>,
    ) -> Result<Box<dyn GlobalSourceState>> {
        let visible_version = i64::try_from(ctx.transaction_visible_version()).unwrap_or(i64::MAX);
        let batch_size = TabletReaderParams::default().batch_size;
        let column_projection =
            if self.bind_data.column_ids.is_empty() && self.bind_data.emit_row_id {
                ColumnProjection::new(Vec::new())
            } else {
                column_pruning::build_column_projection(
                    &self.bind_data.column_ids,
                    self.bind_data.table_data.types().len(),
                )
            };
        let projected_columns = column_projection.read_columns().len().max(1);

        let tmm_cfg = ctx.temporary_memory_manager().current_config();
        let scan_memory_state = ctx.temporary_memory_manager().register();
        let base_scan_budget_cfg = memory_budget::ScanMemoryBudgetConfig::new(
            projected_columns,
            1,
            batch_size,
            ctx.num_threads(),
            tmm_cfg.query_max_memory,
            tmm_cfg.force_external,
        );
        let initial_scan_budget =
            memory_budget::plan_scan_memory_budget(&scan_memory_state, &base_scan_budget_cfg);

        let page_cache = Arc::new(PageCache::new(ctx.buffer_pool().clone()));
        let prefetch_state = ctx.temporary_memory_manager().register();
        let prefetcher = Arc::new(Prefetcher::new(
            page_cache.clone(),
            ctx.session.scheduler().clone(),
            prefetch_state,
            initial_scan_budget.prefetch_options(),
        ));
        prefetcher.update_target_bytes(initial_scan_budget.prefetch_target_bytes);

        let decompressor = ParallelDecompressor::new(ctx.allocator(MemoryTag::ColumnData))
            .with_max_threads(initial_scan_budget.decompress_max_threads);
        let segment_options = SegmentOptions::default()
            .with_page_cache(page_cache)
            .with_parallel_decompressor(decompressor);
        let segments = self
            .bind_data
            .table_data
            .collect_segments_with_options(visible_version, segment_options.clone())?;
        let total_rows = segments.iter().fold(0u64, |acc, (_, segment)| {
            acc.saturating_add(segment.num_rows())
        });

        let scan_budget_config = base_scan_budget_cfg.with_segment_count(segments.len().max(1));
        let scan_budget =
            memory_budget::plan_scan_memory_budget(&scan_memory_state, &scan_budget_config);
        prefetcher.update_target_bytes(scan_budget.prefetch_target_bytes);
        let max_scan_threads = if segments.is_empty() {
            1
        } else {
            scan_budget.max_scan_threads.min(segments.len()).max(1)
        };

        let late_materialize_state = ctx.temporary_memory_manager().register();
        let mut late_materialize_plan = late_materialize::plan_late_materialization(
            self.bind_data.predicate_tree.as_ref(),
            &column_projection,
            batch_size,
            &late_materialize_state,
        );
        if scan_budget.externalize && late_materialize_plan.enabled {
            late_materialize_plan.enabled = false;
            late_materialize_state.set_zero();
        }

        Ok(Box::new(RowsetScanGlobalState {
            segments,
            total_rows,
            rows_scanned: AtomicU64::new(0),
            next_segment: AtomicUsize::new(0),
            max_scan_threads,
            column_projection,
            predicate_tree: self.bind_data.predicate_tree.clone(),
            segment_options: Some(segment_options),
            prefetcher: Some(prefetcher),
            late_materialize_plan: Some(late_materialize_plan),
            late_materialize_state: Some(late_materialize_state),
            scan_memory_state,
            scan_budget_config,
        }))
    }

    fn get_local_source_state(
        &self,
        _ctx: &ExecutionContext,
        _gstate: &dyn GlobalSourceState,
    ) -> Result<Box<dyn LocalSourceState>> {
        Ok(Box::new(RowsetScanLocalState::default()))
    }

    fn get_data(
        &self,
        ctx: &ExecutionContext,
        chunk: &mut Chunk,
        input: &mut OperatorSourceInput,
    ) -> Result<SourceResultType> {
        let gstate = input
            .global_state
            .as_any()
            .downcast_ref::<RowsetScanGlobalState>()
            .ok_or_else(|| paro_error::internal("Invalid global source state".to_string()))?;
        let lstate = input
            .local_state
            .as_any_mut()
            .downcast_mut::<RowsetScanLocalState>()
            .ok_or_else(|| paro_error::internal("Invalid local source state".to_string()))?;

        loop {
            if lstate.reader.is_none() {
                let segment_idx = gstate.next_segment.fetch_add(1, Ordering::SeqCst);
                if segment_idx >= gstate.segments.len() {
                    return Ok(SourceResultType::Finished);
                }

                let (rowset, segment) = &gstate.segments[segment_idx];
                let remaining_segments = gstate.segments.len().saturating_sub(segment_idx).max(1);
                let runtime_budget_cfg = gstate
                    .scan_budget_config
                    .clone()
                    .with_segment_count(remaining_segments);
                let runtime_budget = memory_budget::plan_scan_memory_budget(
                    &gstate.scan_memory_state,
                    &runtime_budget_cfg,
                );
                if runtime_budget.backpressure {
                    std::thread::yield_now();
                }

                lstate.current_segment_total_rows = segment.num_rows();
                lstate.current_segment_output_rows = 0;

                let visible_version =
                    i64::try_from(ctx.transaction_visible_version()).unwrap_or(i64::MAX);
                let mut params = TabletReaderParams::with_version(visible_version)
                    .with_projection(gstate.column_projection.clone())
                    .with_segment(segment.segment_id())
                    .with_emit_row_id(self.bind_data.emit_row_id);
                if let Some(opts) = &gstate.segment_options {
                    params = params.with_segment_options(opts.clone());
                }
                if let Some(prefetcher) = &gstate.prefetcher {
                    prefetcher.update_target_bytes(runtime_budget.prefetch_target_bytes);
                    if runtime_budget.use_prefetch {
                        params = params.with_prefetcher(prefetcher.clone());
                    }
                }
                if let Some(tree) = &gstate.predicate_tree {
                    params = params.with_predicates(tree.clone());
                }
                if let Some(plan) = &gstate.late_materialize_plan {
                    if plan.enabled && !runtime_budget.externalize {
                        params = params.with_late_materialize(plan.predicate_columns.clone());
                    }
                }

                let scan_allocator = ctx.allocator(MemoryTag::ColumnData);
                let mut reader = self
                    .bind_data
                    .table_data
                    .create_reader_with_allocator(params, scan_allocator)?;
                // Manually prepare with only the required rowset to restrict reading to this segment
                reader.prepare_with_rowsets(vec![rowset.clone()])?;
                lstate.reader = Some(reader);
            }

            let reader = lstate.reader.as_mut().expect("reader must be initialized");
            match reader.get_next_chunk()? {
                Some(out) => {
                    let output_rows = out.size() as u64;
                    lstate.current_segment_output_rows = lstate
                        .current_segment_output_rows
                        .saturating_add(output_rows);
                    gstate
                        .rows_scanned
                        .fetch_add(output_rows, Ordering::Relaxed);
                    *chunk = out;
                    return Ok(SourceResultType::HaveMoreOutput);
                }
                None => {
                    let remaining_rows = lstate
                        .current_segment_total_rows
                        .saturating_sub(lstate.current_segment_output_rows);
                    if remaining_rows > 0 {
                        gstate
                            .rows_scanned
                            .fetch_add(remaining_rows, Ordering::Relaxed);
                    }
                    lstate.current_segment_total_rows = 0;
                    lstate.current_segment_output_rows = 0;

                    // Segment finished, try next one
                    lstate.reader = None;
                    continue;
                }
            }
        }
    }

    fn get_progress(&self, gstate: &dyn GlobalSourceState) -> ProgressData {
        let Some(gstate) = gstate.as_any().downcast_ref::<RowsetScanGlobalState>() else {
            return ProgressData::invalid();
        };

        let scanned = gstate.rows_scanned.load(Ordering::Relaxed);
        progress_from_rows(scanned, gstate.total_rows)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_progress_from_rows_empty_source() {
        let progress = progress_from_rows(0, 0);
        assert_eq!(progress.percentage, 1.0);
        assert_eq!(progress.rows_scanned, 0);
    }

    #[test]
    fn test_progress_from_rows_clamps_to_total() {
        let progress = progress_from_rows(120, 100);
        assert_eq!(progress.percentage, 1.0);
        assert_eq!(progress.rows_scanned, 100);
    }

    #[test]
    fn test_progress_from_rows_partial() {
        let progress = progress_from_rows(25, 100);
        assert_eq!(progress.percentage, 0.25);
        assert_eq!(progress.rows_scanned, 25);
    }
}
