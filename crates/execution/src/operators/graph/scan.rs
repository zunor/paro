// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::collections::HashSet;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use paro_catalog::entry::CatalogEntryEnum;
use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::identity::GraphId;
use paro_common::types::LogicalType;
use paro_common::vector::Vector;
use paro_planner::expression::{ColumnRefExpression, Expression};
use paro_planner::operator::ColumnBinding;
use paro_storage::table::table_handle::TableHandle;
use paro_storage::tablet::{TabletReader, TabletReaderParams};
use paro_storage::transaction::overlay_reader::TxnOverlayReader;
use paro_transaction::TableId;

use crate::expression_executor::executor::{ExpressionExecutor, VectorKernelInput};
use crate::operators::sort::build::query_has_temporary_directory;
use crate::physical::specs::GraphScanSpec;
use crate::runtime::context::{OperatorCallContext, PipelineInitContext};
use crate::runtime::source::SourcePoll;
use crate::runtime::state::{
    GraphFilterScanState, GraphScanSourceGlobal, GraphScanSourceLocal, SourceGlobal, SourceLocal,
};
use crate::runtime::{read_u64_from_vector, visit_column_refs, ExpressionEvalInput};

// ---------------------------------------------------------------------------
// Exec struct
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct GraphScanSourceExec {
    pub spec: GraphScanSpec,
}

// ---------------------------------------------------------------------------
// Impl
// ---------------------------------------------------------------------------

impl GraphScanSourceExec {
    pub(crate) fn create_global(&self, ctx: &mut PipelineInitContext) -> Result<SourceGlobal> {
        if ctx.query.session.limits.force_external && !query_has_temporary_directory(ctx.query) {
            return Err(paro_error::out_of_memory(
                "force_external graph scan requires a temporary directory",
            ));
        }
        let snapshot = ctx
            .query
            .session
            .services
            .graph_index
            .snapshot(&GraphId::new(
                ctx.query.session.current_database(),
                &self.spec.schema_name,
                &self.spec.graph_name,
            ))
            .ok_or_else(|| {
                paro_error::internal(format!(
                    "Graph projection index for \"{}\" not found",
                    self.spec.graph_name
                ))
            })?;
        let derived_lag_lease = ctx
            .query
            .session
            .txn
            .lease_derived_lag_if_needed(snapshot.indexed_through_ts())?;
        let snapshot = snapshot.with_derived_lag_lease(derived_lag_lease);
        snapshot.ensure_covers_read_ts(ctx.query.transaction.visible_version())?;
        let num_vertices = snapshot
            .base()
            .vertex_map(&self.spec.label)
            .map(|map| map.num_vertices())
            .unwrap_or(0);
        Ok(SourceGlobal::GraphScan(Arc::new(GraphScanSourceGlobal {
            next_offset: Default::default(),
            snapshot,
            label: self.spec.label.clone(),
            num_vertices,
        })))
    }

    pub(crate) fn create_local(
        &self,
        _ctx: &mut PipelineInitContext,
        global: &SourceGlobal,
    ) -> Result<SourceLocal> {
        graph_scan_global(global)?;
        Ok(SourceLocal::GraphScan(GraphScanSourceLocal::default()))
    }

    pub(crate) fn poll_next(
        &self,
        ctx: &mut OperatorCallContext,
        global: &SourceGlobal,
        local: &mut SourceLocal,
        output: &mut Chunk,
    ) -> Result<SourcePoll> {
        let global = graph_scan_global(global)?;
        let local = graph_scan_local(local)?;
        if local.finished {
            return Ok(SourcePoll::Finished);
        }
        if self.spec.filter.is_some() {
            return poll_graph_scan_filter(ctx, &self.spec, global, local, output);
        }
        if global.num_vertices == 0 {
            local.finished = true;
            return Ok(SourcePoll::Finished);
        }
        let vertex_map = global
            .snapshot
            .base()
            .vertex_map(&global.label)
            .ok_or_else(|| {
                paro_error::internal(format!(
                    "vertex map for label \"{}\" not found",
                    global.label
                ))
            })?;
        const GRAPH_SCAN_BATCH: u32 = 2048;
        let start = global
            .next_offset
            .fetch_add(GRAPH_SCAN_BATCH, Ordering::AcqRel);
        if start >= global.num_vertices {
            local.finished = true;
            return Ok(SourcePoll::Finished);
        }
        let end = start
            .saturating_add(GRAPH_SCAN_BATCH)
            .min(global.num_vertices);
        let batch_size = (end - start) as usize;
        let mut chunk =
            prepare_graph_scan_output(local, output, batch_size, GRAPH_SCAN_BATCH as usize)?;
        let (local_ids, rowids) = graph_scan_output_columns(&mut chunk)?;
        for row_idx in 0..batch_size {
            let local_id = start + row_idx as u32;
            local_ids.set_u64(row_idx, local_id as u64);
            rowids.set_u64(row_idx, vertex_map.local_to_rowid(local_id));
        }
        if end >= global.num_vertices {
            local.finished = true;
        }
        publish_graph_scan_output(local, output, chunk);
        Ok(SourcePoll::Output)
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn poll_graph_scan_filter(
    ctx: &mut OperatorCallContext,
    spec: &GraphScanSpec,
    global: &GraphScanSourceGlobal,
    local: &mut GraphScanSourceLocal,
    output: &mut Chunk,
) -> Result<SourcePoll> {
    if local.filter_scan.is_none() {
        local.filter_scan = Some(init_graph_filter_scan(ctx, spec)?);
    }
    let vertex_map = global
        .snapshot
        .base()
        .vertex_map(&global.label)
        .ok_or_else(|| {
            paro_error::internal(format!(
                "vertex map for label \"{}\" not found",
                global.label
            ))
        })?;
    const GRAPH_SCAN_FILTER_BATCH: usize = 2048;
    let mut chunk = prepare_graph_scan_output(
        local,
        output,
        GRAPH_SCAN_FILTER_BATCH,
        GRAPH_SCAN_FILTER_BATCH,
    )?;
    let (local_ids, rowids) = graph_scan_output_columns(&mut chunk)?;
    let mut out_row = 0usize;
    let filter_scan = local
        .filter_scan
        .as_mut()
        .expect("graph filter scan is initialized");

    loop {
        let need_chunk = match filter_scan.current_chunk.as_ref() {
            Some(chunk) => filter_scan.current_row >= chunk.size(),
            None => true,
        };
        if need_chunk {
            match filter_scan.reader.get_next_chunk()? {
                Some(scan_chunk) if scan_chunk.is_empty() => continue,
                Some(scan_chunk) => {
                    let mut filter_result = Chunk::try_initialize(
                        &[LogicalType::Boolean],
                        scan_chunk.size(),
                        scan_chunk.allocator().clone(),
                    )?;
                    filter_scan.filter_executor.execute_all_kernel(
                        VectorKernelInput::from_eval_input(ExpressionEvalInput {
                            params: ctx.query.params.as_ref(),
                            columns: &scan_chunk,
                        }),
                        ctx.query,
                        &mut filter_result,
                    )?;
                    let bool_col = filter_result
                        .column(0)
                        .ok_or_else(|| {
                            paro_error::internal("missing boolean column from graph filter")
                        })?
                        .clone();
                    filter_scan.current_chunk = Some(scan_chunk);
                    filter_scan.current_filter = Some(bool_col);
                    filter_scan.current_row = 0;
                }
                None => {
                    local.finished = true;
                    break;
                }
            }
        }

        let scan_chunk = filter_scan
            .current_chunk
            .as_ref()
            .expect("graph filter scan chunk loaded");
        let bool_col = filter_scan
            .current_filter
            .as_ref()
            .expect("graph filter result loaded");
        let rowid_col = scan_chunk
            .column(scan_chunk.column_count().saturating_sub(1))
            .ok_or_else(|| paro_error::internal("missing rowid column in graph filter scan"))?;

        while filter_scan.current_row < scan_chunk.size() {
            let row = filter_scan.current_row;
            filter_scan.current_row += 1;
            if !bool_col.get_bool(row).unwrap_or(false) {
                continue;
            }
            let rowid = read_u64_from_vector(rowid_col, row, "graph filter rowid")?;
            if let Some(local_id) = vertex_map.rowid_to_local(rowid) {
                local_ids.set_u64(out_row, local_id as u64);
                rowids.set_u64(out_row, rowid);
                out_row += 1;
                if out_row == GRAPH_SCAN_FILTER_BATCH {
                    break;
                }
            }
        }

        if out_row == GRAPH_SCAN_FILTER_BATCH || local.finished {
            break;
        }
    }

    if out_row == 0 && local.finished {
        local.output = Some(chunk);
        return Ok(SourcePoll::Finished);
    }
    chunk.try_set_cardinality(out_row)?;
    publish_graph_scan_output(local, output, chunk);
    Ok(SourcePoll::Output)
}

fn prepare_graph_scan_output(
    local: &mut GraphScanSourceLocal,
    output: &Chunk,
    row_count: usize,
    capacity_hint: usize,
) -> Result<Chunk> {
    let allocator = output.allocator().clone();
    let capacity = capacity_hint.max(row_count).max(1);
    let types = [LogicalType::UBigInt, LogicalType::UBigInt];
    let mut chunk = match local.output.take() {
        Some(mut chunk) if chunk.column_count() == types.len() && chunk.capacity() >= row_count => {
            chunk.try_reset(allocator)?;
            chunk
        }
        _ => Chunk::try_initialize(&types, capacity, allocator)?,
    };
    chunk.try_set_cardinality(row_count)?;
    Ok(chunk)
}

fn graph_scan_output_columns(chunk: &mut Chunk) -> Result<(&mut Vector, &mut Vector)> {
    let (first, rest) = chunk.data.split_at_mut(1);
    let local_ids = Vector::try_make_arc_mut(&mut first[0])?;
    let rowids = Vector::try_make_arc_mut(
        rest.get_mut(0)
            .ok_or_else(|| paro_error::internal("GraphScan rowid output column missing"))?,
    )?;
    Ok((local_ids, rowids))
}

fn publish_graph_scan_output(
    local: &mut GraphScanSourceLocal,
    output: &mut Chunk,
    mut chunk: Chunk,
) {
    std::mem::swap(output, &mut chunk);
    local.output = Some(chunk);
}

fn init_graph_filter_scan(
    ctx: &mut OperatorCallContext,
    spec: &GraphScanSpec,
) -> Result<GraphFilterScanState> {
    let filter = spec
        .filter
        .as_ref()
        .ok_or_else(|| paro_error::internal("graph filter scan requires a filter expression"))?;
    let table_entry = ctx.query.session.catalog().get_table(
        &ctx.query.catalog,
        &spec.schema_name,
        &spec.vertex_info.table_name,
    )?;
    let table = match table_entry.as_ref() {
        CatalogEntryEnum::Table(table) => table,
        _ => {
            return Err(paro_error::wrong_object_type(
                "table",
                &spec.vertex_info.table_name,
            ))
        }
    };
    let storage = table.get_storage().ok_or_else(|| {
        paro_error::internal(format!(
            "vertex table \"{}\" has no storage",
            spec.vertex_info.table_name
        ))
    })?;
    ctx.query
        .transaction
        .read_tracker()
        .record_table_read(TableId::new(storage.table_id()));

    let column_ids = extract_graph_column_ids(filter);
    let reader = open_runtime_overlay_table_reader(
        storage,
        &ctx.query.transaction,
        column_ids.clone(),
        true,
    )?;
    Ok(GraphFilterScanState {
        reader,
        filter_executor: ExpressionExecutor::with_expressions_for_session(
            &[remap_graph_columns(filter, &column_ids)?],
            ctx.query.session.as_ref(),
        ),
        current_chunk: None,
        current_filter: None,
        current_row: 0,
    })
}

fn open_runtime_overlay_table_reader(
    storage: &TableHandle,
    txn_view: &paro_transaction::TransactionView,
    columns: Vec<usize>,
    emit_row_id: bool,
) -> Result<TabletReader> {
    let snapshot =
        storage.storage_snapshot(txn_view.read_ts(), txn_view.read_snapshot().lease())?;
    let overlay = TxnOverlayReader::for_tablet(&storage.tablet(), txn_view)?;
    let mut rowsets = snapshot.rowsets()?;
    if let Some(overlay) = &overlay {
        let visible_rowsets = rowsets
            .iter()
            .map(|rowset| rowset.rowset_id())
            .collect::<HashSet<_>>();
        rowsets.extend(
            overlay
                .all_rowsets()
                .into_iter()
                .filter(|rowset| !visible_rowsets.contains(&rowset.rowset_id())),
        );
    }
    let mut params = TabletReaderParams::with_version(snapshot.visible_version())
        .with_columns(columns)
        .with_emit_row_id(emit_row_id);
    if let Some(delete_vectors) = overlay.as_ref().and_then(TxnOverlayReader::delete_vectors) {
        params = params.with_overlay_delete_vectors(delete_vectors);
    }
    let mut reader = storage.create_reader(params)?;
    reader.prepare_with_pinned_rowsets(rowsets)?;
    Ok(reader)
}

fn extract_graph_column_ids(expr: &Expression) -> Vec<usize> {
    let mut ids = Vec::new();
    visit_column_refs(expr, &mut |col_ref| {
        ids.push(col_ref.binding.column_index);
    });
    ids.sort_unstable();
    ids.dedup();
    ids
}

fn remap_graph_columns(expr: &Expression, column_ids: &[usize]) -> Result<Expression> {
    let mut missing = None;
    visit_column_refs(expr, &mut |col_ref| {
        let original = col_ref.binding.column_index;
        if !column_ids.contains(&original) {
            missing = Some(original);
        }
    });
    if let Some(column_idx) = missing {
        return Err(paro_error::internal(format!(
            "graph filter column {column_idx} is missing from pruned scan projection"
        )));
    }
    Ok(expr.clone().replace_column_ref(&|col_ref| {
        let original = col_ref.binding.column_index;
        let column_index = column_ids
            .iter()
            .position(|&column_id| column_id == original)
            .expect("graph filter column projection validated");
        Some(Expression::ColumnRef(ColumnRefExpression::new(
            ColumnBinding::new(col_ref.binding.table_index, column_index),
            col_ref.return_type.clone(),
        )))
    }))
}

// ---------------------------------------------------------------------------
// State accessor helpers
// ---------------------------------------------------------------------------

#[inline(always)]
pub(crate) fn graph_scan_global(global: &SourceGlobal) -> Result<&GraphScanSourceGlobal> {
    match global {
        SourceGlobal::GraphScan(state) => Ok(state.as_ref()),
        _ => Err(paro_error::internal(
            "graph scan source global state mismatch",
        )),
    }
}

#[inline(always)]
pub(crate) fn graph_scan_local(local: &mut SourceLocal) -> Result<&mut GraphScanSourceLocal> {
    match local {
        SourceLocal::GraphScan(state) => Ok(state),
        _ => Err(paro_error::internal(
            "graph scan source local state mismatch",
        )),
    }
}
