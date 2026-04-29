// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Physical rowset scan operator.
//!
//! MVP implementation that reads Tablet data via TabletReader using
//! the transaction visible version.

use std::any::Any;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

use crate::execution_context::ExecutionContext;
use crate::explain::explain_node::format_predicate_tree;
use crate::memory_runtime::PrefetchLease;
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
use paro_common::memory::{MemoryAccountingClass, MemoryReleaseHandle};
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use paro_storage::buffer::{PageCache, Prefetcher};
use paro_storage::compression::ParallelDecompressor;
use paro_storage::index::{Predicate, PredicateTree};
use paro_storage::primary_key::{primary_key_hash, PrimaryKeySerializer};
use paro_storage::rowset::{RowsetSharedPtr, SegmentOptions, SegmentSharedPtr};
use paro_storage::table::{table_handle::TableHandle, StorageSnapshot};
use paro_storage::tablet::{ColumnProjection, KeysType, TabletReader, TabletReaderParams};
use paro_storage::transaction::overlay_reader::OverlayDeleteVectorMap;
use paro_transaction::TableId;

const MAX_EXACT_PRIMARY_KEY_READ_KEYS: usize = 1024;

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

fn primary_key_read_hash_ranges(
    table: &TableHandle,
    predicate_tree: Option<&PredicateTree>,
) -> Result<Option<Vec<(u64, u64)>>> {
    let Some(predicate_tree) = predicate_tree else {
        return Ok(None);
    };
    let Some(schema) = table.tablet().schema() else {
        return Ok(None);
    };
    if schema.keys_type() != KeysType::PrimaryKeys {
        return Ok(None);
    }

    let key_column_ids = schema
        .key_columns()
        .iter()
        .map(|column| column.id)
        .collect::<Vec<_>>();
    let Some(keys) = exact_primary_key_values(predicate_tree, &key_column_ids) else {
        return Ok(None);
    };

    let serializer = PrimaryKeySerializer::from_schema_ref(&schema)?;
    let mut ranges = Vec::with_capacity(keys.len());
    for values in keys {
        let key = serializer.encode_values(&values)?;
        let key_hash = primary_key_hash(&key);
        ranges.push((key_hash, key_hash));
    }
    ranges.sort_unstable();
    ranges.dedup();
    Ok(Some(ranges))
}

fn exact_primary_key_values(
    predicate_tree: &PredicateTree,
    key_column_ids: &[u32],
) -> Option<Vec<Vec<Value>>> {
    if key_column_ids.is_empty() {
        return None;
    }

    match predicate_tree {
        PredicateTree::Or(children) => {
            let mut keys = Vec::new();
            for child in children {
                let child_keys = exact_primary_key_values_from_conjunction(child, key_column_ids)?;
                if keys.len().saturating_add(child_keys.len()) > MAX_EXACT_PRIMARY_KEY_READ_KEYS {
                    return None;
                }
                keys.extend(child_keys);
            }
            Some(keys)
        }
        other => exact_primary_key_values_from_conjunction(other, key_column_ids),
    }
}

fn exact_primary_key_values_from_conjunction(
    predicate_tree: &PredicateTree,
    key_column_ids: &[u32],
) -> Option<Vec<Vec<Value>>> {
    let mut constraints = vec![None; key_column_ids.len()];
    if !collect_primary_key_conjunction_constraints(
        predicate_tree,
        key_column_ids,
        &mut constraints,
    ) {
        return None;
    }

    let mut expanded = vec![Vec::with_capacity(key_column_ids.len())];
    for values in constraints {
        let values = values?;
        if values.is_empty() {
            return Some(Vec::new());
        }
        if expanded.len().saturating_mul(values.len()) > MAX_EXACT_PRIMARY_KEY_READ_KEYS {
            return None;
        }

        let mut next = Vec::with_capacity(expanded.len() * values.len());
        for prefix in &expanded {
            for value in &values {
                let mut key = prefix.clone();
                key.push(value.clone());
                next.push(key);
            }
        }
        expanded = next;
    }
    Some(expanded)
}

fn collect_primary_key_conjunction_constraints(
    predicate_tree: &PredicateTree,
    key_column_ids: &[u32],
    constraints: &mut [Option<Vec<Value>>],
) -> bool {
    match predicate_tree {
        PredicateTree::Leaf(predicate) => {
            if let Some((key_idx, values)) = predicate_primary_key_values(predicate, key_column_ids)
            {
                merge_key_constraint(&mut constraints[key_idx], values);
            }
            true
        }
        PredicateTree::And(children) => children.iter().all(|child| {
            collect_primary_key_conjunction_constraints(child, key_column_ids, constraints)
        }),
        PredicateTree::Or(_) => false,
    }
}

fn predicate_primary_key_values(
    predicate: &Predicate,
    key_column_ids: &[u32],
) -> Option<(usize, Vec<Value>)> {
    let (column_id, values) = match predicate {
        Predicate::Eq { column_id, value } => (*column_id, vec![value.clone()]),
        Predicate::In { column_id, values } => (*column_id, values.clone()),
        _ => return None,
    };
    key_column_ids
        .iter()
        .position(|candidate| *candidate == column_id)
        .map(|key_idx| (key_idx, values))
}

fn merge_key_constraint(slot: &mut Option<Vec<Value>>, values: Vec<Value>) {
    let values = values.into_iter().fold(Vec::new(), |mut deduped, value| {
        if !deduped.contains(&value) {
            deduped.push(value);
        }
        deduped
    });

    if let Some(existing) = slot {
        existing.retain(|value| values.contains(value));
    } else {
        *slot = Some(values);
    }
}

#[derive(Debug, Default)]
struct RowsetScanLocalState {
    reader: Option<TabletReader>,
    current_segment_total_rows: u64,
    current_segment_output_rows: u64,
    late_materialize_reservation: Option<MemoryReleaseHandle>,
}

impl LocalSourceState for RowsetScanLocalState {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

impl RowsetScanLocalState {
    fn clear_reader(&mut self) {
        self.reader = None;
        self.late_materialize_reservation = None;
    }
}

#[derive(Debug)]
struct RowsetScanGlobalState {
    storage_snapshot: StorageSnapshot,
    segments: Vec<(RowsetSharedPtr, SegmentSharedPtr)>,
    overlay_delete_vectors: Option<Arc<OverlayDeleteVectorMap>>,
    total_rows: u64,
    rows_scanned: AtomicU64,
    next_segment: AtomicUsize,
    max_scan_threads: usize,
    column_projection: ColumnProjection,
    predicate_tree: Option<PredicateTree>,
    segment_options: Option<SegmentOptions>,
    prefetcher: Option<Arc<Prefetcher>>,
    late_materialize_plan: Option<late_materialize::LateMaterializePlan>,
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
        let txn_view = ctx.transaction_view();
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

        let base_scan_budget_cfg = memory_budget::ScanMemoryBudgetConfig::new(
            projected_columns,
            1,
            batch_size,
            ctx.num_threads(),
            ctx.query_max_memory(),
            ctx.force_external(),
        );
        let initial_scan_budget = memory_budget::plan_scan_memory_budget(&base_scan_budget_cfg);

        let page_cache = Arc::new(PageCache::new(ctx.buffer_pool().clone()));
        let prefetch_lease = Arc::new(PrefetchLease::new(
            ctx.operator_memory_account(),
            initial_scan_budget.prefetch_target_bytes,
        ));
        let prefetcher = Arc::new(Prefetcher::new(
            page_cache.clone(),
            ctx.session.scheduler().clone(),
            prefetch_lease,
            initial_scan_budget.prefetch_options(),
        ));
        prefetcher.update_target_bytes(initial_scan_budget.prefetch_target_bytes);

        let decompressor = ParallelDecompressor::new(ctx.allocator(MemoryTag::ColumnData))
            .with_max_threads(initial_scan_budget.decompress_max_threads);
        let segment_options = SegmentOptions::default()
            .with_page_cache(page_cache)
            .with_parallel_decompressor(decompressor);
        let storage_snapshot = self
            .bind_data
            .table_data
            .storage_snapshot(txn_view.read_ts(), txn_view.read_snapshot().lease())?;
        let materialized_snapshot = storage_snapshot.materialize()?;
        let table_id = TableId::new(self.bind_data.table_data.table_id());
        if let Some(key_ranges) = primary_key_read_hash_ranges(
            &self.bind_data.table_data,
            self.bind_data.predicate_tree.as_ref(),
        )? {
            txn_view
                .read_tracker()
                .record_key_ranges(table_id, key_ranges);
        } else {
            txn_view.read_tracker().record_tablet_read(
                table_id,
                storage_snapshot.tablet_id(),
                storage_snapshot.read_ts(),
                materialized_snapshot.layout_epoch_snapshot,
                materialized_snapshot.rowsets.len(),
            );
        }
        let mut segments = storage_snapshot.segments_with_options(segment_options.clone())?;
        let overlay = paro_storage::transaction::overlay_reader::TxnOverlayReader::for_tablet(
            &self.bind_data.table_data.tablet(),
            txn_view,
        )?;
        let overlay_delete_vectors = overlay
            .as_ref()
            .and_then(|overlay| overlay.delete_vectors());
        if let Some(overlay) = &overlay {
            segments.extend(overlay.segments_with_options(segment_options.clone())?);
        }
        let total_rows = segments.iter().fold(0u64, |acc, (_, segment)| {
            acc.saturating_add(segment.num_rows())
        });

        let scan_budget_config = base_scan_budget_cfg.with_segment_count(segments.len().max(1));
        let scan_budget = memory_budget::plan_scan_memory_budget(&scan_budget_config);
        prefetcher.update_target_bytes(scan_budget.prefetch_target_bytes);
        let max_scan_threads = if segments.is_empty() {
            1
        } else {
            scan_budget.max_scan_threads.min(segments.len()).max(1)
        };

        let mut late_materialize_plan = late_materialize::plan_late_materialization(
            self.bind_data.predicate_tree.as_ref(),
            &column_projection,
            batch_size,
        );
        if scan_budget.externalize && late_materialize_plan.enabled {
            late_materialize_plan.enabled = false;
        }

        Ok(Box::new(RowsetScanGlobalState {
            storage_snapshot,
            segments,
            overlay_delete_vectors,
            total_rows,
            rows_scanned: AtomicU64::new(0),
            next_segment: AtomicUsize::new(0),
            max_scan_threads,
            column_projection,
            predicate_tree: self.bind_data.predicate_tree.clone(),
            segment_options: Some(segment_options),
            prefetcher: Some(prefetcher),
            late_materialize_plan: Some(late_materialize_plan),
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
            ctx.check_cancelled()?;

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
                let runtime_budget = memory_budget::plan_scan_memory_budget(&runtime_budget_cfg);
                if runtime_budget.backpressure {
                    std::thread::yield_now();
                }

                lstate.current_segment_total_rows = segment.num_rows();
                lstate.current_segment_output_rows = 0;

                let mut params =
                    TabletReaderParams::with_version(gstate.storage_snapshot.visible_version())
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
                if let Some(delete_vectors) = &gstate.overlay_delete_vectors {
                    params = params.with_overlay_delete_vectors(delete_vectors.clone());
                }
                if let Some(plan) = &gstate.late_materialize_plan {
                    if plan.enabled && !runtime_budget.externalize {
                        let reservation = input
                            .memory
                            .local_grant()
                            .ok_or_else(|| {
                                paro_error::internal(
                                    "rowset scan late materialization requires tracked memory"
                                        .to_string(),
                                )
                            })?
                            .retain_external_allocation_handle(
                                MemoryTag::ColumnData,
                                MemoryAccountingClass::NonRevocable,
                                plan.required_bytes,
                            )?;
                        params = params.with_late_materialize(plan.predicate_columns.clone());
                        lstate.late_materialize_reservation = Some(reservation);
                    }
                }

                let scan_allocator = ctx.allocator(MemoryTag::ColumnData);
                let mut reader = self
                    .bind_data
                    .table_data
                    .create_reader_with_allocator(params, scan_allocator)?;
                reader.prepare_with_pinned_rowsets(vec![rowset.clone()])?;
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
                    lstate.clear_reader();
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

    #[test]
    fn exact_primary_key_values_extracts_single_point() {
        let predicate = PredicateTree::Leaf(Predicate::Eq {
            column_id: 0,
            value: Value::Integer(7),
        });

        assert_eq!(
            exact_primary_key_values(&predicate, &[0]),
            Some(vec![vec![Value::Integer(7)]])
        );
    }

    #[test]
    fn exact_primary_key_values_extracts_composite_in_product() {
        let predicate = PredicateTree::And(vec![
            PredicateTree::Leaf(Predicate::In {
                column_id: 0,
                values: vec![Value::Integer(1), Value::Integer(2)],
            }),
            PredicateTree::Leaf(Predicate::Eq {
                column_id: 1,
                value: Value::Varchar("a".to_string()),
            }),
            PredicateTree::Leaf(Predicate::Gt {
                column_id: 2,
                value: Value::Integer(10),
            }),
        ]);

        assert_eq!(
            exact_primary_key_values(&predicate, &[0, 1]),
            Some(vec![
                vec![Value::Integer(1), Value::Varchar("a".to_string())],
                vec![Value::Integer(2), Value::Varchar("a".to_string())],
            ])
        );
    }

    #[test]
    fn exact_primary_key_values_rejects_partial_key() {
        let predicate = PredicateTree::Leaf(Predicate::Eq {
            column_id: 0,
            value: Value::Integer(7),
        });

        assert_eq!(exact_primary_key_values(&predicate, &[0, 1]), None);
    }
}
