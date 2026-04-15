//! Graph scan operator for vertex tables.
//!
//! Without a filter it streams all vertices from the `VertexIdMap`. With a
//! filter it scans the backing table, evaluates the predicate, and maps
//! matching rowids back to local vertex ids.

use std::any::Any;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use paro_catalog::entry::{CatalogEntryEnum, VertexTableInfo};
use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::identity::GraphId;
use paro_common::types::LogicalType;
use paro_common::vector::Vector;
use paro_planner::expression::Expression;
use paro_storage::index::graph::{GraphProjectionIndex, GraphReadSnapshot};
use paro_storage::tablet::TabletReaderParams;

use crate::execution_context::ExecutionContext;
use crate::expression_executor::executor::ExpressionExecutor;
use crate::operator::state::{GlobalSourceState, LocalSourceState, OperatorSourceInput};
use crate::operator::PhysicalOperator;
use crate::operator_type::PhysicalOperatorType;
use crate::result_type::SourceResultType;

use super::graph_cardinality::estimate_scan_cardinality;

/// Physical graph scan operator.
///
/// Produces Chunks with two columns: `[local_vertex_id (u64), rowid (u64)]`.
///
/// When `filter` is present, scans the vertex table with column
/// pruning, evaluates the predicate, and only emits vertices that pass.
#[derive(Debug)]
pub struct PhysicalGraphScan {
    /// Graph name for index lookup.
    pub graph_name: String,
    /// Vertex table metadata.
    pub vertex_info: VertexTableInfo,
    /// Vertex label.
    pub label: String,
    /// Optional filter expression on vertex properties.
    pub filter: Option<Expression>,
    /// Schema name for catalog lookup when filter is present.
    pub schema_name: String,
    /// Output types: [UBigInt, UBigInt].
    output_types: Vec<LogicalType>,
}

/// Shared global state for parallel graph scans.
/// Also caches graph projection index per query.
#[derive(Debug, Default)]
struct GraphScanGlobalState {
    next_offset: AtomicU32,
    cached_snapshot: Option<GraphReadSnapshot>,
    num_vertices: u32,
}

impl GlobalSourceState for GraphScanGlobalState {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

/// Thread-local state for GraphScan operator.
///
/// Caches the graph projection index handle and vertex map reference
/// to avoid repeated RwLock acquisitions on the hot path.
/// Supports batched output via global atomic offset and local finished flag.
///
/// When a filter is present, `filter_scan` holds the streaming scan state
/// (reader + current chunk + filter mask), consumed in batches.
#[derive(Debug)]
struct GraphScanLocalState {
    /// Whether we've finished scanning.
    finished: bool,
    /// Streaming filter scan state.
    filter_scan: Option<FilterScanState>,
}

/// Streaming filter scan state for GraphScan.
#[derive(Debug)]
struct FilterScanState {
    reader: paro_storage::tablet::TabletReader,
    filter_exprs: Vec<Expression>,
    current_chunk: Option<Chunk>,
    current_filter: Option<Arc<Vector>>,
    current_row: usize,
}

impl LocalSourceState for GraphScanLocalState {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

impl PhysicalGraphScan {
    pub fn new(
        graph_name: String,
        vertex_info: VertexTableInfo,
        label: String,
        filter: Option<Expression>,
        schema_name: String,
    ) -> Self {
        let output_types = vec![LogicalType::UBigInt, LogicalType::UBigInt];
        Self {
            graph_name,
            vertex_info,
            label,
            filter,
            schema_name,
            output_types,
        }
    }

    /// Initialize streaming filter scan state.
    ///
    /// Uses ExpressionExecutor for filter evaluation and
    /// VertexIdMap::rowid_to_local for rowid→local_id conversion.
    fn init_filter_scan_state(&self, ctx: &ExecutionContext) -> Result<FilterScanState> {
        let filter = self.filter.as_ref().unwrap();

        let catalog = ctx.catalog();
        let txn = ctx.catalog_txn_view();
        let visible_version = i64::try_from(ctx.transaction_visible_version()).unwrap_or(i64::MAX);

        // Get the vertex table from catalog
        let table_entry =
            catalog.get_table(&txn, &self.schema_name, &self.vertex_info.table_name)?;
        let table = match table_entry.as_ref() {
            CatalogEntryEnum::Table(t) => t,
            _ => {
                return Err(paro_error::wrong_object_type(
                    "table",
                    &self.vertex_info.table_name,
                ));
            }
        };
        let storage = table.get_storage().ok_or_else(|| {
            paro_error::internal(format!(
                "Vertex table \"{}\" has no storage",
                self.vertex_info.table_name
            ))
        })?;

        // Extract column IDs referenced by the filter for column pruning.
        let filter_col_ids = extract_column_ids(filter);

        // Build scan params: read only filter-relevant columns + rowid
        let params = TabletReaderParams::with_version(visible_version)
            .with_columns(filter_col_ids.clone())
            .with_emit_row_id(true);
        let mut reader = storage.create_reader(params)?;
        reader.prepare()?;

        // Build the filter expression for evaluation.
        // The filter uses ColumnRefExpression with (table_index, column_index)
        // where column_index refers to the column position in the full vertex table.
        // Since we're reading a subset of columns, we need to remap column_index
        // to the position in our pruned scan output.
        let remapped_filter = remap_filter_columns(filter, &filter_col_ids);
        let filter_exprs = vec![remapped_filter];

        Ok(FilterScanState {
            reader,
            filter_exprs,
            current_chunk: None,
            current_filter: None,
            current_row: 0,
        })
    }
}

impl PhysicalOperator for PhysicalGraphScan {
    fn operator_type(&self) -> PhysicalOperatorType {
        PhysicalOperatorType::GraphScan
    }

    fn types(&self) -> &[LogicalType] {
        &self.output_types
    }

    fn explain_params(&self) -> Vec<String> {
        let mut params = vec![
            format!("Graph: {}", self.graph_name),
            format!("Vertex Label: {}", self.label),
            format!("Table: {}", self.vertex_info.table_name),
        ];
        if self.filter.is_some() {
            params.push("Filter: <pushed down>".to_string());
        }
        params
    }

    fn estimated_cardinality(&self) -> usize {
        estimate_scan_cardinality(self.filter.as_ref(), &self.vertex_info)
    }

    fn is_source(&self) -> bool {
        true
    }

    fn parallel_source(&self) -> bool {
        // Parallelize the no-filter path with atomic offsets.
        // Filtered scans remain single-threaded until range scan is supported.
        self.filter.is_none()
    }

    fn get_global_source_state(
        &self,
        ctx: &ExecutionContext,
        _sink_state: Option<&dyn crate::operator::state::GlobalSinkState>,
    ) -> Result<Box<dyn GlobalSourceState>> {
        // Cache graph projection index once per query.
        let snapshot = ctx
            .session
            .services
            .graph_index
            .snapshot(&GraphId::new(
                ctx.session.current_database(),
                &self.schema_name,
                &self.graph_name,
            ))
            .ok_or_else(|| {
                paro_error::internal(format!(
                    "Graph projection index for \"{}\" not found",
                    self.graph_name
                ))
            })?;

        let num_vertices = snapshot
            .base()
            .vertex_map(&self.label)
            .map(|vm| vm.num_vertices())
            .unwrap_or(0);

        Ok(Box::new(GraphScanGlobalState {
            next_offset: AtomicU32::new(0),
            cached_snapshot: Some(snapshot),
            num_vertices,
        }))
    }

    fn get_local_source_state(
        &self,
        _ctx: &ExecutionContext,
        _gstate: &dyn GlobalSourceState,
    ) -> Result<Box<dyn LocalSourceState>> {
        Ok(Box::new(GraphScanLocalState {
            finished: false,
            filter_scan: None,
        }))
    }

    fn get_data(
        &self,
        ctx: &ExecutionContext,
        chunk: &mut Chunk,
        input: &mut OperatorSourceInput,
    ) -> Result<SourceResultType> {
        let state = input
            .local_state
            .as_any_mut()
            .downcast_mut::<GraphScanLocalState>()
            .expect("Invalid state type for GraphScan");

        if state.finished {
            return Ok(SourceResultType::Finished);
        }

        let gstate = input
            .global_state
            .as_any()
            .downcast_ref::<GraphScanGlobalState>()
            .ok_or_else(|| paro_error::internal("Invalid global state for GraphScan"))?;

        // Use cached index handle (acquired in get_global_source_state)
        let snapshot = gstate.cached_snapshot.as_ref().ok_or_else(|| {
            paro_error::internal(format!(
                "Graph read snapshot for \"{}\" not cached in global state",
                self.graph_name
            ))
        })?;
        let index = snapshot.base().as_ref();

        // If filter is present, use filter-based scanning
        if self.filter.is_some() {
            return self.get_data_with_filter(ctx, chunk, state, index);
        }

        // No filter: parallel scan all vertices from VertexIdMap
        let gstate = input
            .global_state
            .as_any()
            .downcast_ref::<GraphScanGlobalState>()
            .ok_or_else(|| paro_error::internal("Invalid global state for GraphScan"))?;
        let vertex_map = index.vertex_map(&self.label).ok_or_else(|| {
            paro_error::internal(format!(
                "Vertex map for label \"{}\" not found in graph \"{}\"",
                self.label, self.graph_name
            ))
        })?;

        let num_vertices = gstate.num_vertices;

        if num_vertices == 0 {
            state.finished = true;
            return Ok(SourceResultType::Finished);
        }

        const GRAPH_SCAN_BATCH: u32 = 2048;
        let start = gstate
            .next_offset
            .fetch_add(GRAPH_SCAN_BATCH, Ordering::SeqCst);
        if start >= num_vertices {
            state.finished = true;
            return Ok(SourceResultType::Finished);
        }
        let end = std::cmp::min(start.saturating_add(GRAPH_SCAN_BATCH), num_vertices);
        let batch_size = (end - start) as usize;
        let mut local_ids = Vector::with_capacity(LogicalType::UBigInt, batch_size);
        let mut rowids = Vector::with_capacity(LogicalType::UBigInt, batch_size);
        local_ids.set_len(batch_size);
        rowids.set_len(batch_size);

        for i in 0..batch_size {
            let local_id = start + i as u32;
            let rowid = vertex_map.local_to_rowid(local_id);
            local_ids.set_u64(i, local_id as u64);
            rowids.set_u64(i, rowid);
        }

        if end >= num_vertices {
            state.finished = true;
        }

        *chunk = Chunk::from_arc_vectors(vec![Arc::new(local_ids), Arc::new(rowids)]);
        chunk.set_cardinality(batch_size);

        if state.finished {
            Ok(SourceResultType::Finished)
        } else {
            Ok(SourceResultType::HaveMoreOutput)
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

impl PhysicalGraphScan {
    /// Filter-based get_data path.
    ///
    /// On first call, evaluates the filter against the vertex table and
    /// caches the matching (local_id, rowid) pairs. Subsequent calls
    /// consume the cached results in batches of 2048.
    fn get_data_with_filter(
        &self,
        ctx: &ExecutionContext,
        chunk: &mut Chunk,
        state: &mut GraphScanLocalState,
        index: &GraphProjectionIndex,
    ) -> Result<SourceResultType> {
        // Lazy initialization of streaming filter scan state
        if state.filter_scan.is_none() {
            let scan_state = self.init_filter_scan_state(ctx)?;
            state.filter_scan = Some(scan_state);
        }

        let scan_state = state.filter_scan.as_mut().unwrap();

        // Resolve vertex map for rowid -> local_id conversion
        let vertex_map = index.vertex_map(&self.label).ok_or_else(|| {
            paro_error::internal(format!(
                "Vertex map for label \"{}\" not found in graph \"{}\"",
                self.label, self.graph_name
            ))
        })?;

        let mut local_ids = Vector::with_capacity(LogicalType::UBigInt, 2048);
        let mut rowids = Vector::with_capacity(LogicalType::UBigInt, 2048);
        local_ids.set_len(2048);
        rowids.set_len(2048);
        let mut out_row = 0usize;

        loop {
            // Load a new scan chunk if needed
            let need_chunk = match scan_state.current_chunk.as_ref() {
                Some(c) => scan_state.current_row >= c.size(),
                None => true,
            };
            if need_chunk {
                if let Some(scan_chunk) = scan_state.reader.get_next_chunk()? {
                    let scan_size = scan_chunk.size();
                    if scan_size == 0 {
                        continue;
                    }
                    // Evaluate filter on this chunk
                    let mut filter_executor =
                        ExpressionExecutor::with_expressions(&scan_state.filter_exprs);
                    let mut filter_result = Chunk::initialize(&[LogicalType::Boolean], scan_size);
                    filter_executor.execute_all_into(&scan_chunk, ctx, &mut filter_result)?;
                    let bool_col = filter_result.column(0).ok_or_else(|| {
                        paro_error::internal("Missing boolean column from filter evaluation")
                    })?;

                    scan_state.current_chunk = Some(scan_chunk);
                    scan_state.current_filter = Some(bool_col.clone());
                    scan_state.current_row = 0;
                } else {
                    // No more data
                    state.finished = true;
                    break;
                }
            }

            let scan_chunk = scan_state.current_chunk.as_ref().unwrap();
            let bool_col = scan_state.current_filter.as_ref().unwrap();
            let rowid_col_idx = scan_chunk.column_count() - 1;
            let rowid_col = scan_chunk
                .column(rowid_col_idx)
                .ok_or_else(|| paro_error::internal("Missing rowid column in vertex scan"))?;
            let scan_size = scan_chunk.size();

            while scan_state.current_row < scan_size {
                let row = scan_state.current_row;
                scan_state.current_row += 1;
                let passes = bool_col.get_bool(row).unwrap_or(false);
                if passes {
                    let rowid = rowid_col.get_i64(row).unwrap_or(0) as u64;
                    if let Some(local_id) = vertex_map.rowid_to_local(rowid) {
                        local_ids.set_u64(out_row, local_id as u64);
                        rowids.set_u64(out_row, rowid);
                        out_row += 1;
                        if out_row >= 2048 {
                            break;
                        }
                    }
                }
            }

            if out_row >= 2048 {
                break;
            }

            // If we exhausted current chunk, loop to fetch next
            if scan_state.current_row >= scan_size {
                continue;
            }
        }

        if out_row == 0 && state.finished {
            return Ok(SourceResultType::Finished);
        }

        local_ids.set_len(out_row);
        rowids.set_len(out_row);
        *chunk = Chunk::from_arc_vectors(vec![Arc::new(local_ids), Arc::new(rowids)]);
        chunk.set_cardinality(out_row);

        if state.finished {
            Ok(SourceResultType::Finished)
        } else {
            Ok(SourceResultType::HaveMoreOutput)
        }
    }
}

/// Extract column IDs referenced by a filter expression.
///
/// Walks the expression tree and collects all `column_index` values
/// from `ColumnRefExpression` nodes. Returns a sorted, deduplicated
/// list of column indices suitable for `TabletReaderParams::with_columns`.
fn extract_column_ids(expr: &Expression) -> Vec<usize> {
    let mut ids = Vec::new();
    collect_column_ids_recursive(expr, &mut ids);
    ids.sort();
    ids.dedup();
    ids
}

fn collect_column_ids_recursive(expr: &Expression, ids: &mut Vec<usize>) {
    match expr {
        Expression::ColumnRef(col_ref) => {
            ids.push(col_ref.binding.column_index);
        }
        Expression::Comparison(cmp) => {
            collect_column_ids_recursive(&cmp.left, ids);
            collect_column_ids_recursive(&cmp.right, ids);
        }
        Expression::Conjunction(conj) => {
            for child in &conj.children {
                collect_column_ids_recursive(child, ids);
            }
        }
        Expression::Function(func) => {
            for child in &func.children {
                collect_column_ids_recursive(child, ids);
            }
        }
        Expression::Cast(cast) => {
            collect_column_ids_recursive(&cast.child, ids);
        }
        Expression::Operator(op) => {
            for child in &op.children {
                collect_column_ids_recursive(child, ids);
            }
        }
        _ => {}
    }
}

/// Remap column references in a filter expression to match the pruned
/// scan output column positions.
///
/// When we scan with `with_columns(col_ids)`, the output columns are
/// ordered by `col_ids`. This function remaps `column_index` in the
/// expression to the position within `col_ids`.
fn remap_filter_columns(expr: &Expression, col_ids: &[usize]) -> Expression {
    use paro_planner::expression::ColumnRefExpression;
    use paro_planner::operator::ColumnBinding;

    match expr {
        Expression::ColumnRef(col_ref) => {
            let original_col = col_ref.binding.column_index;
            let new_col = col_ids
                .iter()
                .position(|&id| id == original_col)
                .unwrap_or(0);
            let new_binding = ColumnBinding::new(col_ref.binding.table_index, new_col);
            Expression::ColumnRef(ColumnRefExpression::new(
                new_binding,
                col_ref.return_type.clone(),
            ))
        }
        Expression::Comparison(cmp) => {
            let mut new_cmp = cmp.clone();
            new_cmp.left = Box::new(remap_filter_columns(&cmp.left, col_ids));
            new_cmp.right = Box::new(remap_filter_columns(&cmp.right, col_ids));
            Expression::Comparison(new_cmp)
        }
        Expression::Conjunction(conj) => {
            let mut new_conj = conj.clone();
            new_conj.children = conj
                .children
                .iter()
                .map(|c| remap_filter_columns(c, col_ids))
                .collect();
            Expression::Conjunction(new_conj)
        }
        Expression::Function(func) => {
            let mut new_func = func.clone();
            new_func.children = func
                .children
                .iter()
                .map(|c| remap_filter_columns(c, col_ids))
                .collect();
            Expression::Function(new_func)
        }
        Expression::Cast(cast) => {
            let mut new_cast = cast.clone();
            new_cast.child = Box::new(remap_filter_columns(&cast.child, col_ids));
            Expression::Cast(new_cast)
        }
        Expression::Operator(op) => {
            let mut new_op = op.clone();
            new_op.children = op
                .children
                .iter()
                .map(|c| remap_filter_columns(c, col_ids))
                .collect();
            Expression::Operator(new_op)
        }
        other => other.clone(),
    }
}
