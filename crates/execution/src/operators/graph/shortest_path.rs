// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::collections::VecDeque;
use std::sync::Arc;

use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::identity::GraphId;
use paro_common::types::LogicalType;
use paro_common::vector::{SelectionVector, VectorSelection};
use paro_parser::ast::PathMode;
use paro_planner::operator::graph_expand::{graph_path_element_list_type, ExpandDirection};

use crate::operators::graph::state::{graph_path_list_value, GraphPathPayload};
use crate::operators::sort::build::query_has_temporary_directory;
use crate::physical::specs::GraphShortestPathSpec;
use crate::runtime::context::{OperatorCallContext, OperatorFinishContext, PipelineInitContext};
use crate::runtime::state::{
    GraphShortestPathTransformGlobal, GraphShortestPathTransformLocal, TransformGlobal,
    TransformLocal,
};
use crate::runtime::transform::{TransformFinishPoll, TransformFlushPoll, TransformPoll};
use crate::runtime::{read_u32_from_vector, read_u64_from_vector};

#[derive(Debug, Clone)]
pub struct GraphShortestPathTransformExec {
    pub spec: GraphShortestPathSpec,
}

#[derive(Debug)]
struct ShortestPathRow {
    input_row: usize,
    edge_rowid: u64,
    dst_local: u32,
    path: Option<GraphPathPayload>,
}

impl GraphShortestPathTransformExec {
    pub(crate) fn create_global(&self, ctx: &mut PipelineInitContext) -> Result<TransformGlobal> {
        if ctx.query.session.limits.force_external && !query_has_temporary_directory(ctx.query) {
            return Err(paro_error::out_of_memory(
                "force_external graph shortest path requires a temporary directory",
            ));
        }
        if self.spec.target_filter.is_some() {
            return Err(paro_error::not_implemented(
                "typed GraphShortestPath target filters require graph target materialization",
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
        Ok(TransformGlobal::GraphShortestPath(Arc::new(
            GraphShortestPathTransformGlobal { snapshot },
        )))
    }

    pub(crate) fn create_local(
        &self,
        _ctx: &mut PipelineInitContext,
        global: &TransformGlobal,
    ) -> Result<TransformLocal> {
        graph_shortest_path_global(global)?;
        Ok(TransformLocal::GraphShortestPath(
            GraphShortestPathTransformLocal {
                ready: VecDeque::new(),
                forward_scratch: Vec::new(),
                backward_scratch: Vec::new(),
                shortest_depths: Vec::new(),
                frontier: VecDeque::new(),
                next_frontier: VecDeque::new(),
                path_frontier: VecDeque::new(),
                path_next_frontier: VecDeque::new(),
            },
        ))
    }

    pub(crate) fn transform(
        &self,
        _ctx: &mut OperatorCallContext,
        global: &TransformGlobal,
        local: &mut TransformLocal,
        input: &Chunk,
        output: &mut Chunk,
    ) -> Result<TransformPoll> {
        let global = graph_shortest_path_global(global)?;
        let local = graph_shortest_path_local(local)?;
        if let Some(mut ready) = local.ready.pop_front() {
            output.move_from(&mut ready);
            return Ok(if local.ready.is_empty() {
                TransformPoll::Output
            } else {
                TransformPoll::OutputMore
            });
        }
        if input.is_empty() {
            return Ok(TransformPoll::NeedMoreInput);
        }
        let mut chunk = build_graph_shortest_path_output(&self.spec, global, local, input, output)?;
        if chunk.is_empty() {
            return Ok(TransformPoll::NeedMoreInput);
        }
        output.move_from(&mut chunk);
        Ok(TransformPoll::Output)
    }

    pub(crate) fn flush(
        &self,
        _ctx: &mut OperatorCallContext,
        _global: &TransformGlobal,
        local: &mut TransformLocal,
        output: &mut Chunk,
    ) -> Result<TransformFlushPoll> {
        let local = graph_shortest_path_local(local)?;
        if let Some(mut ready) = local.ready.pop_front() {
            output.move_from(&mut ready);
            return Ok(if local.ready.is_empty() {
                TransformFlushPoll::Output
            } else {
                TransformFlushPoll::OutputMore
            });
        }
        Ok(TransformFlushPoll::Done)
    }

    pub(crate) fn finish_global(
        &self,
        _ctx: &mut OperatorFinishContext,
        _global: &TransformGlobal,
    ) -> Result<TransformFinishPoll> {
        Ok(TransformFinishPoll::Done)
    }
}

#[inline(always)]
fn graph_shortest_path_global(
    global: &TransformGlobal,
) -> Result<&GraphShortestPathTransformGlobal> {
    match global {
        TransformGlobal::GraphShortestPath(state) => Ok(state.as_ref()),
        _ => Err(paro_error::internal(
            "graph shortest path global state mismatch",
        )),
    }
}

#[inline(always)]
fn graph_shortest_path_local(
    local: &mut TransformLocal,
) -> Result<&mut GraphShortestPathTransformLocal> {
    match local {
        TransformLocal::GraphShortestPath(state) => Ok(state),
        _ => Err(paro_error::internal(
            "graph shortest path local state mismatch",
        )),
    }
}

fn build_graph_shortest_path_output(
    spec: &GraphShortestPathSpec,
    global: &GraphShortestPathTransformGlobal,
    local: &mut GraphShortestPathTransformLocal,
    input: &Chunk,
    output: &Chunk,
) -> Result<Chunk> {
    let source_col = input
        .column(spec.source_local_col_idx)
        .ok_or_else(|| paro_error::internal("GraphShortestPath source local id column missing"))?;
    let source_rowid_col = spec
        .has_path_functions
        .then(|| {
            input.column(spec.source_rowid_col_idx).ok_or_else(|| {
                paro_error::internal(
                    "GraphShortestPath source rowid column missing for path functions",
                )
            })
        })
        .transpose()?;
    let target_col = spec
        .target_local_col_idx
        .and_then(|target_col_idx| input.column(target_col_idx));
    let target_map = global
        .snapshot
        .base()
        .vertex_map(&spec.target_label)
        .ok_or_else(|| {
            paro_error::internal(format!(
                "target vertex map for label \"{}\" not found",
                spec.target_label
            ))
        })?;
    let num_vertices = target_map.num_vertices() as usize;
    let mut rows = Vec::new();

    for row_idx in 0..input.size() {
        let source =
            read_u32_from_vector(source_col, row_idx, "GraphShortestPath source local id")?;
        let source_rowid = source_rowid_col
            .map(|column| read_u64_from_vector(column, row_idx, "GraphShortestPath source rowid"))
            .transpose()?;
        let bound_target = target_col
            .map(|column| {
                read_u32_from_vector(column, row_idx, "GraphShortestPath target local id")
            })
            .transpose()?;
        append_shortest_path_rows(
            spec,
            global,
            local,
            num_vertices,
            target_map.local_to_rowids(),
            row_idx,
            source,
            source_rowid,
            bound_target,
            &mut rows,
        )?;
    }

    let mut output_types = if spec.output_types.is_empty() {
        let mut types = input.types();
        types.extend_from_slice(&[
            LogicalType::UBigInt,
            LogicalType::UBigInt,
            LogicalType::UBigInt,
        ]);
        types
    } else {
        spec.output_types.to_vec()
    };
    let required_columns = input.column_count() + 3 + usize::from(spec.has_path_functions) * 3;
    if output_types.len() < required_columns {
        output_types = input.types();
        output_types.extend_from_slice(&[
            LogicalType::UBigInt,
            LogicalType::UBigInt,
            LogicalType::UBigInt,
        ]);
        if spec.has_path_functions {
            output_types.extend_from_slice(&[
                LogicalType::BigInt,
                graph_path_element_list_type(),
                graph_path_element_list_type(),
            ]);
        }
    }

    let allocator = output.allocator().clone();
    let row_count = rows.len();
    let mut chunk = Chunk::try_initialize(&output_types, row_count, allocator)?;
    copy_selected_input_columns(
        input,
        &mut chunk,
        rows.iter().map(|row| row.input_row),
        row_count,
    )?;
    for (out_idx, row) in rows.into_iter().enumerate() {
        let dst_rowid = target_map.local_to_rowid(row.dst_local);
        let base = input.column_count();
        chunk
            .column_mut(base)
            .ok_or_else(|| paro_error::internal("GraphShortestPath edge output column missing"))?
            .set_u64(out_idx, row.edge_rowid);
        chunk
            .column_mut(base + 1)
            .ok_or_else(|| {
                paro_error::internal("GraphShortestPath target local output column missing")
            })?
            .set_u64(out_idx, row.dst_local as u64);
        chunk
            .column_mut(base + 2)
            .ok_or_else(|| {
                paro_error::internal("GraphShortestPath target rowid output column missing")
            })?
            .set_u64(out_idx, dst_rowid);
        if let Some(path) = &row.path {
            chunk
                .column_mut(base + 3)
                .ok_or_else(|| {
                    paro_error::internal("GraphShortestPath path length column missing")
                })?
                .set_i64(out_idx, path.hop_count());
            chunk
                .column_mut(base + 4)
                .ok_or_else(|| {
                    paro_error::internal("GraphShortestPath path vertices column missing")
                })?
                .set_value(out_idx, &graph_path_list_value(&path.vertices));
            chunk
                .column_mut(base + 5)
                .ok_or_else(|| paro_error::internal("GraphShortestPath path edges column missing"))?
                .set_value(out_idx, &graph_path_list_value(&path.edges));
        }
    }
    chunk.try_set_cardinality(row_count)?;
    Ok(chunk)
}

fn append_shortest_path_rows(
    spec: &GraphShortestPathSpec,
    global: &GraphShortestPathTransformGlobal,
    local: &mut GraphShortestPathTransformLocal,
    num_vertices: usize,
    target_rowids: &[u64],
    input_row: usize,
    source: u32,
    source_rowid: Option<u64>,
    bound_target: Option<u32>,
    rows: &mut Vec<ShortestPathRow>,
) -> Result<()> {
    if num_vertices == 0 || source as usize >= num_vertices {
        return Ok(());
    }
    if spec.max_hops == 0 {
        return Ok(());
    }

    let min_hops = spec.min_hops.max(1) as usize;
    let max_hops = spec.max_hops.min(usize::MAX as u64) as usize;
    if local.shortest_depths.len() < num_vertices {
        local.shortest_depths.resize(num_vertices, u64::MAX);
    }
    local.shortest_depths.fill(u64::MAX);
    local.shortest_depths[source as usize] = 0;
    local.frontier.clear();
    local.next_frontier.clear();
    local.path_frontier.clear();
    local.path_next_frontier.clear();
    local.frontier.push_back(source);
    if spec.has_path_functions {
        let source_rowid = source_rowid.ok_or_else(|| {
            paro_error::internal("GraphShortestPath source rowid is required for path functions")
        })?;
        local
            .path_frontier
            .push_back(GraphPathPayload::root(spec.source_table_oid, source_rowid));
    }
    let all_shortest = matches!(spec.path_mode, Some(PathMode::AllShortest));
    let shortest_depths = &mut local.shortest_depths;
    let frontier = &mut local.frontier;
    let next_frontier = &mut local.next_frontier;
    let path_frontier = &mut local.path_frontier;
    let path_next_frontier = &mut local.path_next_frontier;
    let forward_scratch = &mut local.forward_scratch;
    let backward_scratch = &mut local.backward_scratch;
    for depth in 1..=max_hops {
        while let Some(vertex) = frontier.pop_front() {
            let path = if spec.has_path_functions {
                Some(path_frontier.pop_front().ok_or_else(|| {
                    paro_error::internal("GraphShortestPath path frontier is out of sync")
                })?)
            } else {
                None
            };
            visit_shortest_path_neighbors(
                spec,
                global,
                forward_scratch,
                backward_scratch,
                vertex,
                &mut |dst, edge_rowid| -> Result<bool> {
                    let dst_idx = dst as usize;
                    if dst_idx >= shortest_depths.len() {
                        return Ok(false);
                    }
                    let depth_u64 = depth as u64;
                    let current_best = shortest_depths[dst_idx];
                    if current_best < depth_u64 || (current_best == depth_u64 && !all_shortest) {
                        return Ok(false);
                    }
                    if current_best == u64::MAX {
                        shortest_depths[dst_idx] = depth_u64;
                        next_frontier.push_back(dst);
                    }
                    let next_path = if let Some(path) = path.as_ref() {
                        let dst_rowid = target_rowids.get(dst_idx).copied().ok_or_else(|| {
                            paro_error::internal(
                                "GraphShortestPath target rowid missing for path functions",
                            )
                        })?;
                        Some(path.extend(
                            spec.edge_info.table_oid,
                            edge_rowid,
                            spec.target_table_oid,
                            dst_rowid,
                        ))
                    } else {
                        None
                    };
                    if current_best == u64::MAX {
                        if let Some(next_path) = next_path.clone() {
                            path_next_frontier.push_back(next_path);
                        }
                    }
                    let target_matches = bound_target.map_or(true, |target| target == dst);
                    if target_matches && depth >= min_hops {
                        rows.push(ShortestPathRow {
                            input_row,
                            edge_rowid,
                            dst_local: dst,
                            path: next_path,
                        });
                        if bound_target.is_some() && !all_shortest {
                            return Ok(true);
                        }
                    }
                    Ok(false)
                },
            )?;
            if bound_target.is_some()
                && !all_shortest
                && rows.last().is_some_and(|row| {
                    row.input_row == input_row && Some(row.dst_local) == bound_target
                })
            {
                return Ok(());
            }
        }
        if next_frontier.is_empty() {
            break;
        }
        std::mem::swap(frontier, next_frontier);
        if spec.has_path_functions {
            std::mem::swap(path_frontier, path_next_frontier);
        }
    }
    Ok(())
}

fn visit_shortest_path_neighbors<F>(
    spec: &GraphShortestPathSpec,
    global: &GraphShortestPathTransformGlobal,
    forward_scratch: &mut Vec<(u32, u64)>,
    backward_scratch: &mut Vec<(u32, u64)>,
    vertex: u32,
    visit: &mut F,
) -> Result<()>
where
    F: FnMut(u32, u64) -> Result<bool>,
{
    if matches!(
        spec.direction,
        ExpandDirection::Forward | ExpandDirection::Both
    ) {
        if let Some(view) =
            global
                .snapshot
                .neighbors_forward(&spec.edge_info.label, vertex, forward_scratch)
        {
            for idx in 0..view.len() {
                if let Some((dst, edge_rowid)) = view.pair_at(idx) {
                    if visit(dst, edge_rowid)? {
                        return Ok(());
                    }
                }
            }
        }
    }
    if matches!(
        spec.direction,
        ExpandDirection::Backward | ExpandDirection::Both
    ) {
        if let Some(view) =
            global
                .snapshot
                .neighbors_backward(&spec.edge_info.label, vertex, backward_scratch)
        {
            for idx in 0..view.len() {
                if let Some((dst, edge_rowid)) = view.pair_at(idx) {
                    if visit(dst, edge_rowid)? {
                        return Ok(());
                    }
                }
            }
        }
    }
    Ok(())
}

fn copy_selected_input_columns<I>(
    input: &Chunk,
    output: &mut Chunk,
    input_rows: I,
    row_count: usize,
) -> Result<()>
where
    I: IntoIterator<Item = usize>,
{
    if row_count == 0 {
        return Ok(());
    }
    let mut selection = SelectionVector::try_with_capacity(row_count, output.allocator().clone())?;
    selection.set_len(row_count);
    for (out_idx, input_row) in input_rows.into_iter().enumerate() {
        selection.try_set(out_idx, input_row)?;
    }
    let selection = VectorSelection::materialized(selection);
    for col_idx in 0..input.column_count() {
        let source = input
            .column(col_idx)
            .ok_or_else(|| paro_error::internal("graph input column missing"))?;
        output
            .column_mut(col_idx)
            .ok_or_else(|| paro_error::internal("graph output column missing"))?
            .try_copy_selection(0, source, &selection, row_count)?;
    }
    Ok(())
}
