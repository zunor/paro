// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::identity::GraphId;
use paro_common::types::LogicalType;
use paro_common::vector::{SelectionVector, VectorSelection, VECTOR_SIZE};
use paro_planner::operator::graph_expand::ExpandDirection;
use paro_storage::index::graph::NeighborView;

use crate::operators::graph::state::{graph_path_list_value, GraphExpandRow, GraphPathPayload};
use crate::operators::sort::build::query_has_temporary_directory;
use crate::physical::specs::GraphExpandSpec;
use crate::runtime::context::{OperatorCallContext, OperatorFinishContext, PipelineInitContext};
use crate::runtime::state::{
    GraphExpandTransformGlobal, GraphExpandTransformLocal, TransformGlobal, TransformLocal,
};
use crate::runtime::transform::{TransformFinishPoll, TransformFlushPoll, TransformPoll};
use crate::runtime::{read_u32_from_vector, read_u64_from_vector};

#[derive(Debug, Clone)]
pub struct GraphExpandTransformExec {
    pub spec: GraphExpandSpec,
}

impl GraphExpandTransformExec {
    pub(crate) fn create_global(&self, ctx: &mut PipelineInitContext) -> Result<TransformGlobal> {
        if ctx.query.session.limits.force_external && !query_has_temporary_directory(ctx.query) {
            return Err(paro_error::out_of_memory(
                "force_external graph expand requires a temporary directory",
            ));
        }
        if self.spec.edge_filter.is_some() || self.spec.target_filter.is_some() {
            return Err(paro_error::not_implemented(
                "typed GraphExpand path filters require RowFetchProject hand-off",
            ));
        }
        if (self.spec.min_hops != 1 || self.spec.max_hops != 1)
            && self.spec.source_label != self.spec.target_label
        {
            return Err(paro_error::not_supported(
                "multi-hop GraphExpand currently requires source and target labels to match",
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
        let target_map = snapshot
            .base()
            .vertex_map(&self.spec.target_label)
            .ok_or_else(|| {
                paro_error::internal(format!(
                    "target vertex map for label \"{}\" not found",
                    self.spec.target_label
                ))
            })?;
        let target_rowids = Arc::<[u64]>::from(target_map.local_to_rowids());
        let target_vertex_count = target_map.num_vertices() as usize;
        Ok(TransformGlobal::GraphExpand(Arc::new(
            GraphExpandTransformGlobal {
                snapshot,
                target_rowids,
                target_vertex_count,
            },
        )))
    }

    pub(crate) fn create_local(
        &self,
        _ctx: &mut PipelineInitContext,
        global: &TransformGlobal,
    ) -> Result<TransformLocal> {
        graph_expand_global(global)?;
        Ok(TransformLocal::GraphExpand(
            GraphExpandTransformLocal::default(),
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
        let global = graph_expand_global(global)?;
        let local = graph_expand_local(local)?;
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
        let row_count = build_graph_expand_output(&self.spec, global, local, input, output)?;
        if row_count == 0 {
            return Ok(TransformPoll::NeedMoreInput);
        }
        Ok(if local.ready.is_empty() {
            TransformPoll::Output
        } else {
            TransformPoll::OutputMore
        })
    }

    pub(crate) fn flush(
        &self,
        _ctx: &mut OperatorCallContext,
        _global: &TransformGlobal,
        local: &mut TransformLocal,
        output: &mut Chunk,
    ) -> Result<TransformFlushPoll> {
        let local = graph_expand_local(local)?;
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
fn graph_expand_global(global: &TransformGlobal) -> Result<&GraphExpandTransformGlobal> {
    match global {
        TransformGlobal::GraphExpand(state) => Ok(state.as_ref()),
        _ => Err(paro_error::internal("graph expand global state mismatch")),
    }
}

#[inline(always)]
fn graph_expand_local(local: &mut TransformLocal) -> Result<&mut GraphExpandTransformLocal> {
    match local {
        TransformLocal::GraphExpand(state) => Ok(state),
        _ => Err(paro_error::internal("graph expand local state mismatch")),
    }
}

fn build_graph_expand_output(
    spec: &GraphExpandSpec,
    global: &GraphExpandTransformGlobal,
    local: &mut GraphExpandTransformLocal,
    input: &Chunk,
    output: &mut Chunk,
) -> Result<usize> {
    let source_col = input
        .column(spec.source_local_col_idx)
        .ok_or_else(|| paro_error::internal("GraphExpand source local id column missing"))?;
    let source_rowid_col = spec
        .has_path_functions
        .then(|| {
            input.column(spec.source_rowid_col_idx).ok_or_else(|| {
                paro_error::internal("GraphExpand source rowid column missing for path functions")
            })
        })
        .transpose()?;
    let visited_len = global.target_vertex_count;
    let effective_max_hops = effective_max_hops(spec.max_hops, visited_len);
    local.rows.clear();
    if effective_max_hops < spec.min_hops && spec.min_hops != 0 {
        prepare_graph_expand_output(output, &spec.output_types, 0)?;
        output.try_set_cardinality(0)?;
        return Ok(0);
    }
    for row_idx in 0..input.size() {
        let source = read_u32_from_vector(source_col, row_idx, "GraphExpand source local id")?;
        let source_rowid = source_rowid_col
            .map(|column| read_u64_from_vector(column, row_idx, "GraphExpand source rowid"))
            .transpose()?;
        collect_graph_expand_rows_for_source(
            spec,
            global,
            local,
            row_idx,
            source,
            source_rowid,
            visited_len,
            effective_max_hops,
        )?;
    }
    let row_count = local.rows.len();
    emit_graph_expand_chunks(spec, input, output, local)?;
    Ok(row_count)
}

fn effective_max_hops(max_hops: u64, vertex_count: usize) -> u64 {
    if max_hops == u64::MAX {
        vertex_count as u64
    } else {
        max_hops
    }
}

fn collect_graph_expand_rows_for_source(
    spec: &GraphExpandSpec,
    global: &GraphExpandTransformGlobal,
    local: &mut GraphExpandTransformLocal,
    input_row: usize,
    source: u32,
    source_rowid: Option<u64>,
    visited_len: usize,
    effective_max_hops: u64,
) -> Result<()> {
    begin_graph_expand_generation(local, visited_len);
    if spec.source_label == spec.target_label {
        mark_graph_expand_unvisited(&mut local.seen_generation, local.current_generation, source);
    }
    if spec.min_hops == 0 && spec.source_label == spec.target_label {
        if let Some(source_rowid) = target_rowid(&global.target_rowids, source) {
            local.rows.push(GraphExpandRow {
                input_row,
                edge_rowid: 0,
                dst_local: source,
                dst_rowid: source_rowid,
                path: spec
                    .has_path_functions
                    .then(|| GraphPathPayload::root(spec.source_table_oid, source_rowid)),
            });
        }
    }
    if effective_max_hops == 0 {
        return Ok(());
    }

    local.frontier.clear();
    local.frontier.push(source);
    local.path_frontier.clear();
    if spec.has_path_functions {
        let source_rowid = source_rowid.ok_or_else(|| {
            paro_error::internal("GraphExpand source rowid is required for path functions")
        })?;
        local
            .path_frontier
            .push(GraphPathPayload::root(spec.source_table_oid, source_rowid));
    }
    for depth in 1..=effective_max_hops {
        if local.frontier.is_empty() {
            break;
        }
        local.next_frontier.clear();
        local.path_next_frontier.clear();
        for idx in 0..local.frontier.len() {
            let vertex = local.frontier[idx];
            let path = if spec.has_path_functions {
                Some(local.path_frontier.get(idx).ok_or_else(|| {
                    paro_error::internal("GraphExpand path frontier is out of sync")
                })?)
            } else {
                None
            };
            if matches!(
                spec.direction,
                ExpandDirection::Forward | ExpandDirection::Both
            ) {
                append_graph_expand_neighbors(
                    spec,
                    global.snapshot.neighbors_forward(
                        &spec.edge_info.label,
                        vertex,
                        &mut local.forward_scratch,
                    ),
                    &global.target_rowids,
                    input_row,
                    depth,
                    effective_max_hops,
                    &mut local.seen_generation,
                    local.current_generation,
                    &mut local.next_frontier,
                    &mut local.path_next_frontier,
                    &mut local.rows,
                    path,
                );
            }
            if matches!(
                spec.direction,
                ExpandDirection::Backward | ExpandDirection::Both
            ) {
                append_graph_expand_neighbors(
                    spec,
                    global.snapshot.neighbors_backward(
                        &spec.edge_info.label,
                        vertex,
                        &mut local.backward_scratch,
                    ),
                    &global.target_rowids,
                    input_row,
                    depth,
                    effective_max_hops,
                    &mut local.seen_generation,
                    local.current_generation,
                    &mut local.next_frontier,
                    &mut local.path_next_frontier,
                    &mut local.rows,
                    path,
                );
            }
        }
        std::mem::swap(&mut local.frontier, &mut local.next_frontier);
        if spec.has_path_functions {
            std::mem::swap(&mut local.path_frontier, &mut local.path_next_frontier);
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn append_graph_expand_neighbors(
    spec: &GraphExpandSpec,
    view: Option<NeighborView<'_>>,
    target_rowids: &[u64],
    input_row: usize,
    depth: u64,
    effective_max_hops: u64,
    seen_generation: &mut [u32],
    current_generation: u32,
    next_frontier: &mut Vec<u32>,
    path_next_frontier: &mut Vec<GraphPathPayload>,
    rows: &mut Vec<GraphExpandRow>,
    path: Option<&GraphPathPayload>,
) {
    let Some(view) = view else {
        return;
    };
    for idx in 0..view.len() {
        let Some((dst, edge_rowid)) = view.pair_at(idx) else {
            continue;
        };
        let Some(dst_rowid) = target_rowid(target_rowids, dst) else {
            continue;
        };
        if effective_max_hops > 1
            && !mark_graph_expand_unvisited(seen_generation, current_generation, dst)
        {
            continue;
        }
        let next_path = path.map(|path| {
            path.extend(
                spec.edge_info.table_oid,
                edge_rowid,
                spec.target_table_oid,
                dst_rowid,
            )
        });
        if depth >= spec.min_hops {
            rows.push(GraphExpandRow {
                input_row,
                edge_rowid,
                dst_local: dst,
                dst_rowid,
                path: next_path.clone(),
            });
        }
        if depth < effective_max_hops {
            next_frontier.push(dst);
            if let Some(next_path) = next_path {
                path_next_frontier.push(next_path);
            }
        }
    }
}

fn begin_graph_expand_generation(local: &mut GraphExpandTransformLocal, visited_len: usize) {
    let required = visited_len.max(1);
    if local.seen_generation.len() < required {
        local.seen_generation.resize(required, 0);
    }
    if local.current_generation == u32::MAX {
        local.seen_generation.fill(0);
        local.current_generation = 1;
    } else {
        local.current_generation += 1;
    }
}

fn mark_graph_expand_unvisited(
    seen_generation: &mut [u32],
    current_generation: u32,
    vertex: u32,
) -> bool {
    let Some(slot) = seen_generation.get_mut(vertex as usize) else {
        return false;
    };
    if *slot == current_generation {
        return false;
    }
    *slot = current_generation;
    true
}

#[inline]
fn target_rowid(target_rowids: &[u64], local_id: u32) -> Option<u64> {
    target_rowids.get(local_id as usize).copied()
}

fn emit_graph_expand_chunks(
    spec: &GraphExpandSpec,
    input: &Chunk,
    output: &mut Chunk,
    local: &mut GraphExpandTransformLocal,
) -> Result<()> {
    local.ready.clear();
    let row_count = local.rows.len();
    if row_count == 0 {
        prepare_graph_expand_output(output, &spec.output_types, 0)?;
        output.try_set_cardinality(0)?;
        return Ok(());
    }

    let mut offset = 0usize;
    let first_count = row_count.min(VECTOR_SIZE);
    write_graph_expand_chunk(
        input,
        output,
        &spec.output_types,
        &local.rows[offset..offset + first_count],
        &mut local.input_selection,
    )?;
    offset += first_count;

    while offset < row_count {
        let count = (row_count - offset).min(VECTOR_SIZE);
        let mut chunk =
            Chunk::try_initialize(&spec.output_types, count.max(1), output.allocator().clone())?;
        write_graph_expand_chunk(
            input,
            &mut chunk,
            &spec.output_types,
            &local.rows[offset..offset + count],
            &mut local.input_selection,
        )?;
        local.ready.push_back(chunk);
        offset += count;
    }
    local.rows.clear();
    Ok(())
}

fn write_graph_expand_chunk(
    input: &Chunk,
    output: &mut Chunk,
    output_types: &[LogicalType],
    rows: &[GraphExpandRow],
    selection_slot: &mut Option<SelectionVector>,
) -> Result<()> {
    let row_count = rows.len();
    prepare_graph_expand_output(output, output_types, row_count)?;
    copy_selected_input_columns(
        input,
        output,
        rows.iter().map(|row| row.input_row),
        row_count,
        selection_slot,
    )?;
    for (out_idx, row) in rows.iter().enumerate() {
        let base = input.column_count();
        output
            .column_mut(base)
            .ok_or_else(|| paro_error::internal("GraphExpand edge output column missing"))?
            .set_u64(out_idx, row.edge_rowid);
        output
            .column_mut(base + 1)
            .ok_or_else(|| paro_error::internal("GraphExpand target local output column missing"))?
            .set_u64(out_idx, row.dst_local as u64);
        output
            .column_mut(base + 2)
            .ok_or_else(|| paro_error::internal("GraphExpand target rowid output column missing"))?
            .set_u64(out_idx, row.dst_rowid);
        if let Some(path) = &row.path {
            output
                .column_mut(base + 3)
                .ok_or_else(|| paro_error::internal("GraphExpand path length column missing"))?
                .set_i64(out_idx, path.hop_count());
            output
                .column_mut(base + 4)
                .ok_or_else(|| paro_error::internal("GraphExpand path vertices column missing"))?
                .set_value(out_idx, &graph_path_list_value(&path.vertices));
            output
                .column_mut(base + 5)
                .ok_or_else(|| paro_error::internal("GraphExpand path edges column missing"))?
                .set_value(out_idx, &graph_path_list_value(&path.edges));
        }
    }
    output.try_set_cardinality(row_count)?;
    Ok(())
}

fn prepare_graph_expand_output(
    output: &mut Chunk,
    output_types: &[LogicalType],
    row_count: usize,
) -> Result<()> {
    let required_capacity = row_count.max(1);
    let matches_shape = output.column_count() == output_types.len()
        && output
            .data
            .iter()
            .zip(output_types.iter())
            .all(|(vector, ty)| vector.logical_type() == ty);
    if !matches_shape || output.capacity() < required_capacity {
        let allocator = output.allocator().clone();
        *output = Chunk::try_initialize(output_types, required_capacity, allocator)?;
        return Ok(());
    }
    output.try_reset(output.allocator().clone())?;
    Ok(())
}

fn copy_selected_input_columns<I>(
    input: &Chunk,
    output: &mut Chunk,
    input_rows: I,
    row_count: usize,
    selection_slot: &mut Option<SelectionVector>,
) -> Result<()>
where
    I: IntoIterator<Item = usize>,
{
    if row_count == 0 {
        return Ok(());
    }
    if selection_slot
        .as_ref()
        .map_or(true, |selection| selection.capacity() < row_count)
    {
        *selection_slot = Some(SelectionVector::try_with_capacity(
            row_count,
            output.allocator().clone(),
        )?);
    }
    let selection = selection_slot
        .as_mut()
        .expect("graph expand selection vector was initialized above");
    selection.set_len(row_count);
    for (out_idx, input_row) in input_rows.into_iter().enumerate() {
        selection.try_set(out_idx, input_row)?;
    }
    let selection = VectorSelection::materialized(selection.clone());
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

#[cfg(test)]
mod tests {
    use paro_catalog::entry::EdgeTableInfo;

    use super::*;

    #[test]
    fn one_hop_expand_emits_parallel_edges_to_visited_vertices() {
        let spec = test_expand_spec(1, 1);
        let neighbors = [0_u32, 1];
        let edge_rowids = [10_u64, 11];
        let target_rowids = [100_u64, 101];
        let mut seen = [1_u32, 0];
        let mut next_frontier = Vec::new();
        let mut path_next_frontier = Vec::new();
        let mut rows = Vec::new();

        append_graph_expand_neighbors(
            &spec,
            Some(NeighborView::Base {
                neighbors: &neighbors,
                edge_rowids: &edge_rowids,
            }),
            &target_rowids,
            0,
            1,
            1,
            &mut seen,
            1,
            &mut next_frontier,
            &mut path_next_frontier,
            &mut rows,
            None,
        );

        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].dst_rowid, 100);
        assert_eq!(rows[1].dst_rowid, 101);
        assert!(next_frontier.is_empty());
    }

    #[test]
    fn multi_hop_expand_uses_seen_set_for_emitted_vertices() {
        let spec = test_expand_spec(1, 2);
        let neighbors = [0_u32, 1];
        let edge_rowids = [10_u64, 11];
        let target_rowids = [100_u64, 101];
        let mut seen = [1_u32, 0];
        let mut next_frontier = Vec::new();
        let mut path_next_frontier = Vec::new();
        let mut rows = Vec::new();

        append_graph_expand_neighbors(
            &spec,
            Some(NeighborView::Base {
                neighbors: &neighbors,
                edge_rowids: &edge_rowids,
            }),
            &target_rowids,
            0,
            1,
            2,
            &mut seen,
            1,
            &mut next_frontier,
            &mut path_next_frontier,
            &mut rows,
            None,
        );

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].dst_rowid, 101);
        assert_eq!(next_frontier, vec![1]);
    }

    fn test_expand_spec(min_hops: u64, max_hops: u64) -> GraphExpandSpec {
        GraphExpandSpec {
            graph_name: "g".to_string(),
            schema_name: "public".to_string(),
            edge_info: EdgeTableInfo {
                table_name: "edge".to_string(),
                table_oid: 1,
                key_column_ids: vec![0],
                source_key_column_ids: vec![0],
                source_vertex_table: "node".to_string(),
                source_ref_column_ids: vec![1],
                destination_key_column_ids: vec![0],
                destination_vertex_table: "node".to_string(),
                destination_ref_column_ids: vec![2],
                label: "E".to_string(),
                property_column_ids: vec![],
            },
            direction: ExpandDirection::Forward,
            source_label: "Node".to_string(),
            edge_filter: None,
            target_filter: None,
            source_table_index: 0,
            edge_table_index: 1,
            target_table_index: 2,
            target_label: "Node".to_string(),
            source_local_col_idx: 0,
            source_rowid_col_idx: 1,
            min_hops,
            max_hops,
            source_table_oid: 10,
            target_table_oid: 10,
            target_table_name: "node".to_string(),
            has_path_functions: false,
            output_names: Box::new([]),
            output_types: Box::new([]),
        }
    }
}
