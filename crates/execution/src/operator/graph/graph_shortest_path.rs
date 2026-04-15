//! Physical Graph Shortest Path Operator
//!
//! BFS-based shortest path operator for SQL/PGQ `ANY SHORTEST` and
//! `ALL SHORTEST` path modes.
//!
//! From a set of start vertices, expands layer by layer (BFS) until
//! reaching target vertices. Only returns paths at the shortest distance.
//!
//! ## SIMD-style parallelization
//!
//! Inspired by DuckPGQ's LANE_LIMIT=64 approach: uses a `u64` bitset to
//! track up to 64 source vertices simultaneously during BFS. Each bit
//! position corresponds to one source vertex, allowing a single BFS pass
//! to compute shortest paths for 64 sources at once.

use std::any::Any;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

use paro_catalog::entry::{CatalogEntryEnum, EdgeTableInfo};
use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::identity::GraphId;
use paro_common::types::LogicalType;
use paro_common::vector::{Vector, VECTOR_SIZE};
use paro_parser::ast::PathMode;
use paro_planner::expression::Expression;
use paro_planner::operator::ExpandDirection;
use paro_storage::buffer::TemporaryMemoryState;
use paro_storage::index::graph::vertex_id_map::VertexIdMap;
use paro_storage::index::graph::{GraphProjectionIndex, GraphReadSnapshot};
use paro_storage::metrics::storage_metrics;
use paro_storage::tablet::TabletReaderParams;

use crate::execution_context::ExecutionContext;
use crate::explain::types::ExplainRuntimeStats;
use crate::expression_executor::executor::ExpressionExecutor;
use crate::operator::state::{GlobalOperatorState, OperatorState};
use crate::operator::PhysicalOperator;
use crate::operator_type::PhysicalOperatorType;
use crate::result_type::OperatorResultType;

use super::graph_cardinality::estimate_expand_cardinality;
use super::graph_output_buffer::{EmitAction, GraphPathOutputBuffer};
use super::graph_path::{
    collect_prefix_path, materialize_path_vectors, path_element_list_type, MaterializedPath,
    PathElementRef, PathEmitSpec, PATH_EDGES_OFFSET, PATH_LENGTH_OFFSET, PATH_VERTICES_OFFSET,
};
use super::spillable_frontier::{SpillableFrontier, SpillableFrontierCursor};
use super::spillable_parent_arrays::{ParentLookupState, SpillableParentArrays};

/// Number of source vertices processed simultaneously using bitset BFS.
const LANE_LIMIT: usize = 64;
const GRAPH_FRONTIER_SPILL_THRESHOLD_ROWS: usize = VECTOR_SIZE * 4;

#[derive(Debug)]
pub struct PhysicalGraphShortestPath {
    pub graph_name: String,
    pub edge_info: EdgeTableInfo,
    pub direction: ExpandDirection,
    pub source_label: String,
    pub target_label: String,
    pub source_local_col_idx: usize,
    /// When present, ANY SHORTEST can use a bound-target bidirectional BFS.
    pub target_local_col_idx: Option<usize>,
    pub path_mode: PathMode,
    pub min_hops: u64,
    pub max_hops: u64,
    /// Whether to emit path metadata columns after the standard output.
    pub emit_path_info: bool,
    /// Prefix path layout used to reconstruct vertices(p) / edges(p).
    pub path_emit_spec: PathEmitSpec,
    /// Optional filter on target vertex properties.
    pub target_filter: Option<Expression>,
    /// Target vertex table name for target filter evaluation.
    pub target_table_name: String,
    /// Schema name for target filter evaluation.
    pub schema_name: String,
    child: Arc<dyn PhysicalOperator>,
    output_types: Vec<LogicalType>,
    externalized: AtomicBool,
    peak_memory_bytes: AtomicUsize,
}

/// BFS state for lane-parallel shortest path computation.
///
/// Holds cached graph index handle for shortest path execution.
///
#[derive(Debug)]
struct GraphShortestPathState {
    /// Cached graph projection index handle (acquired once, avoids RwLock on hot path).
    cached_snapshot: Option<GraphReadSnapshot>,
    /// Scratch space for forward delta-aware neighbor merges.
    forward_neighbor_scratch: Vec<(u32, u64)>,
    /// Scratch space for backward delta-aware neighbor merges.
    backward_neighbor_scratch: Vec<(u32, u64)>,
    /// Optional singleton target bitset used for bound-target shortest path.
    valid_targets: Option<Vec<u64>>,
    valid_targets_computed: bool,
    output_buffer: GraphPathOutputBuffer,
    resume_state: Option<GraphShortestPathResumeState>,
    input_row_cursor: usize,
    lane_seen_scratch: Vec<u64>,
    lane_visit_scratch: Vec<u64>,
    lane_visit_next_scratch: Vec<u64>,
    temporary_memory_state: Option<Arc<TemporaryMemoryState>>,
}

#[derive(Debug)]
enum GraphShortestPathResumeState {
    Lane(LaneBfsState),
    SingleSource(SingleSourceBfsState),
}

#[derive(Debug)]
struct BidirectionalDirectionState {
    current_hop: u32,
    frontier: SpillableFrontier,
    next_frontier: SpillableFrontier,
    frontier_cursor: SpillableFrontierCursor,
    seen: Vec<u64>,
    visited_depth: Vec<u32>,
    terminal_edge_to_root: Vec<u64>,
    parents: Option<SpillableParentArrays>,
}

#[derive(Debug)]
struct BoundTargetSearchResult {
    meet: u32,
    path_len: u64,
    terminal_edge_rowid: u64,
}

#[derive(Debug)]
struct PendingLaneEmission {
    remaining_bits: u64,
    edge_rowid: u64,
    dst: u32,
}

#[derive(Debug)]
enum LaneAdvanceResult {
    Suspended(LaneBfsState),
    Finished {
        seen: Vec<u64>,
        visit: Vec<u64>,
        visit_next: Vec<u64>,
    },
}

#[derive(Debug)]
struct LaneBfsState {
    sources: Vec<(Vec<u64>, u32)>,
    seen: Vec<u64>,
    visit: Vec<u64>,
    visit_next: Vec<u64>,
    frontier: Vec<u32>,
    next_frontier: Vec<u32>,
    frontier_idx: usize,
    neighbor_direction: u8,
    neighbor_idx: usize,
    current_hop: u64,
    pending_emit: Option<PendingLaneEmission>,
}

#[derive(Debug)]
struct SingleSourceBfsState {
    row: usize,
    input_vals: Vec<u64>,
    seen: Vec<bool>,
    next_seen: Vec<bool>,
    frontier: SpillableFrontier,
    frontier_cursor: SpillableFrontierCursor,
    next_frontier: SpillableFrontier,
    parents: SpillableParentArrays,
    parent_lookup_state: ParentLookupState,
    frontier_idx: usize,
    neighbor_direction: u8,
    neighbor_idx: usize,
    current_hop: u64,
}

impl OperatorState for GraphShortestPathState {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

impl PhysicalGraphShortestPath {
    fn ensure_temporary_memory_state(
        &self,
        ctx: &ExecutionContext,
        op_state: &mut GraphShortestPathState,
    ) -> Arc<TemporaryMemoryState> {
        if let Some(temp_state) = &op_state.temporary_memory_state {
            return temp_state.clone();
        }
        let temp_state = ctx.temporary_memory_manager().register();
        temp_state.set_zero();
        op_state.temporary_memory_state = Some(temp_state.clone());
        temp_state
    }

    fn lane_workset_bytes(state: &LaneBfsState) -> usize {
        state.seen.len() * std::mem::size_of::<u64>()
            + state.visit.len() * std::mem::size_of::<u64>()
            + state.visit_next.len() * std::mem::size_of::<u64>()
            + state.frontier.len() * std::mem::size_of::<u32>()
            + state.next_frontier.len() * std::mem::size_of::<u32>()
    }

    fn single_source_workset_bytes(state: &SingleSourceBfsState) -> usize {
        state.frontier.resident_memory_bytes()
            + state.next_frontier.resident_memory_bytes()
            + state.parents.current_in_memory_bytes()
    }

    fn bidirectional_workset_bytes(
        from_src: &BidirectionalDirectionState,
        from_dst: &BidirectionalDirectionState,
    ) -> usize {
        let src_parent_bytes = from_src
            .parents
            .as_ref()
            .map(|parents| parents.current_in_memory_bytes())
            .unwrap_or(0);
        let dst_parent_bytes = from_dst
            .parents
            .as_ref()
            .map(|parents| parents.current_in_memory_bytes())
            .unwrap_or(0);
        from_src.frontier.resident_memory_bytes()
            + from_src.next_frontier.resident_memory_bytes()
            + from_dst.frontier.resident_memory_bytes()
            + from_dst.next_frontier.resident_memory_bytes()
            + src_parent_bytes
            + dst_parent_bytes
    }

    fn update_lane_temporary_memory(
        &self,
        temp_state: &Arc<TemporaryMemoryState>,
        state: &LaneBfsState,
    ) {
        let bytes = Self::lane_workset_bytes(state);
        if bytes == 0 {
            temp_state.set_zero();
        } else {
            temp_state.set_remaining_size_and_update_reservation(bytes);
        }
        self.record_runtime_memory(temp_state, false);
    }

    fn update_single_source_temporary_memory(
        &self,
        temp_state: &Arc<TemporaryMemoryState>,
        state: &mut SingleSourceBfsState,
    ) -> Result<()> {
        let mut bytes = Self::single_source_workset_bytes(state);
        if bytes == 0 {
            temp_state.set_zero();
            return Ok(());
        }
        temp_state.set_remaining_size_and_update_reservation(bytes);
        self.record_runtime_memory(
            temp_state,
            state.frontier.is_external() || state.next_frontier.is_external(),
        );
        if temp_state.get_reservation() < bytes {
            state.frontier.ensure_external()?;
            state.next_frontier.ensure_external()?;
            bytes = Self::single_source_workset_bytes(state);
            if bytes == 0 {
                temp_state.set_zero();
            } else {
                temp_state.set_remaining_size_and_update_reservation(bytes);
                self.record_runtime_memory(
                    temp_state,
                    state.frontier.is_external() || state.next_frontier.is_external(),
                );
            }
        }
        Ok(())
    }

    fn update_bound_target_temporary_memory(
        &self,
        temp_state: &Arc<TemporaryMemoryState>,
        from_src: &mut BidirectionalDirectionState,
        from_dst: &mut BidirectionalDirectionState,
    ) -> Result<()> {
        let mut bytes = Self::bidirectional_workset_bytes(from_src, from_dst);
        if bytes == 0 {
            temp_state.set_zero();
            return Ok(());
        }
        temp_state.set_remaining_size_and_update_reservation(bytes);
        self.record_runtime_memory(
            temp_state,
            from_src.frontier.is_external()
                || from_src.next_frontier.is_external()
                || from_dst.frontier.is_external()
                || from_dst.next_frontier.is_external(),
        );
        if temp_state.get_reservation() < bytes {
            from_src.frontier.ensure_external()?;
            from_src.next_frontier.ensure_external()?;
            from_dst.frontier.ensure_external()?;
            from_dst.next_frontier.ensure_external()?;
            bytes = Self::bidirectional_workset_bytes(from_src, from_dst);
            if bytes == 0 {
                temp_state.set_zero();
            } else {
                temp_state.set_remaining_size_and_update_reservation(bytes);
                self.record_runtime_memory(
                    temp_state,
                    from_src.frontier.is_external()
                        || from_src.next_frontier.is_external()
                        || from_dst.frontier.is_external()
                        || from_dst.next_frontier.is_external(),
                );
            }
        }
        Ok(())
    }

    fn graph_frontier_threshold(
        &self,
        ctx: &ExecutionContext,
        requires_spillable_frontier: bool,
    ) -> Result<usize> {
        let tmm_cfg = ctx.temporary_memory_manager().current_config();
        if requires_spillable_frontier && tmm_cfg.force_external && !tmm_cfg.has_temporary_directory
        {
            return Err(paro_error::out_of_memory(
                "force_external requires a temporary directory (SET temp_directory)",
            ));
        }
        Ok(if requires_spillable_frontier && tmm_cfg.force_external {
            0
        } else {
            GRAPH_FRONTIER_SPILL_THRESHOLD_ROWS
        })
    }

    fn record_runtime_memory(&self, temp_state: &Arc<TemporaryMemoryState>, externalized: bool) {
        self.peak_memory_bytes
            .fetch_max(temp_state.get_peak_reservation(), Ordering::AcqRel);
        if externalized {
            self.externalized.store(true, Ordering::Release);
        }
    }

    pub fn new(
        graph_name: String,
        edge_info: EdgeTableInfo,
        direction: ExpandDirection,
        source_label: String,
        target_label: String,
        source_local_col_idx: usize,
        target_local_col_idx: Option<usize>,
        path_mode: PathMode,
        min_hops: u64,
        max_hops: u64,
        child: Arc<dyn PhysicalOperator>,
    ) -> Self {
        Self::with_path_info_and_filter(
            graph_name,
            edge_info,
            direction,
            source_label,
            target_label,
            source_local_col_idx,
            target_local_col_idx,
            path_mode,
            min_hops,
            max_hops,
            false,
            PathEmitSpec::default(),
            None,
            String::new(),
            String::new(),
            child,
        )
    }

    pub fn with_path_info(
        graph_name: String,
        edge_info: EdgeTableInfo,
        direction: ExpandDirection,
        source_label: String,
        target_label: String,
        source_local_col_idx: usize,
        target_local_col_idx: Option<usize>,
        path_mode: PathMode,
        min_hops: u64,
        max_hops: u64,
        emit_path_info: bool,
        path_emit_spec: PathEmitSpec,
        child: Arc<dyn PhysicalOperator>,
    ) -> Self {
        Self::with_path_info_and_filter(
            graph_name,
            edge_info,
            direction,
            source_label,
            target_label,
            source_local_col_idx,
            target_local_col_idx,
            path_mode,
            min_hops,
            max_hops,
            emit_path_info,
            path_emit_spec,
            None,
            String::new(),
            String::new(),
            child,
        )
    }

    pub fn with_path_info_and_filter(
        graph_name: String,
        edge_info: EdgeTableInfo,
        direction: ExpandDirection,
        source_label: String,
        target_label: String,
        source_local_col_idx: usize,
        target_local_col_idx: Option<usize>,
        path_mode: PathMode,
        min_hops: u64,
        max_hops: u64,
        emit_path_info: bool,
        path_emit_spec: PathEmitSpec,
        target_filter: Option<Expression>,
        target_table_name: String,
        schema_name: String,
        child: Arc<dyn PhysicalOperator>,
    ) -> Self {
        let mut output_types = child.types().to_vec();
        // Append 3 columns: [edge_rowid, dst_local_id, dst_rowid]
        output_types.push(LogicalType::UBigInt);
        output_types.push(LogicalType::UBigInt);
        output_types.push(LogicalType::UBigInt);
        // Optional path metadata: [path_length, vertices(p), edges(p)]
        if emit_path_info {
            output_types.push(LogicalType::BigInt);
            output_types.push(path_element_list_type());
            output_types.push(path_element_list_type());
        }
        Self {
            graph_name,
            edge_info,
            direction,
            source_label,
            target_label,
            source_local_col_idx,
            target_local_col_idx,
            path_mode,
            min_hops,
            max_hops,
            emit_path_info,
            path_emit_spec,
            target_filter,
            target_table_name,
            schema_name,
            child,
            output_types,
            externalized: AtomicBool::new(false),
            peak_memory_bytes: AtomicUsize::new(0),
        }
    }

    fn collect_input_row_values(input: &Chunk, row: usize) -> Vec<u64> {
        let mut vals = Vec::with_capacity(input.column_count());
        for c in 0..input.column_count() {
            vals.push(input.column(c).and_then(|v| v.get_u64(row)).unwrap_or(0));
        }
        vals
    }

    fn init_lane_state(
        &self,
        sources: Vec<(Vec<u64>, u32)>,
        num_vertices: u32,
        lane_seen_scratch: &mut Vec<u64>,
        lane_visit_scratch: &mut Vec<u64>,
        lane_visit_next_scratch: &mut Vec<u64>,
    ) -> Option<LaneBfsState> {
        if sources.is_empty() || num_vertices == 0 {
            return None;
        }

        let nv = num_vertices as usize;
        let mut seen = std::mem::take(lane_seen_scratch);
        seen.resize(nv, 0);
        seen.fill(0);
        let mut visit = std::mem::take(lane_visit_scratch);
        visit.resize(nv, 0);
        visit.fill(0);
        let mut visit_next = std::mem::take(lane_visit_next_scratch);
        visit_next.resize(nv, 0);
        visit_next.fill(0);
        let mut frontier = Vec::new();

        for (lane, (_, src_local)) in sources.iter().enumerate() {
            let bit = 1u64 << lane;
            let idx = *src_local as usize;
            if idx >= nv {
                continue;
            }
            if visit[idx] == 0 {
                frontier.push(*src_local);
            }
            seen[idx] |= bit;
            visit[idx] |= bit;
        }

        if frontier.is_empty() {
            return None;
        }

        Some(LaneBfsState {
            sources,
            seen,
            visit,
            visit_next,
            frontier,
            next_frontier: Vec::new(),
            frontier_idx: 0,
            neighbor_direction: 0,
            neighbor_idx: 0,
            current_hop: 1,
            pending_emit: None,
        })
    }

    fn emit_lane_rows(
        &self,
        sources: &[(Vec<u64>, u32)],
        vmap: &VertexIdMap,
        bits: u64,
        edge_rowid: u64,
        dst: u32,
        buffer: &mut GraphPathOutputBuffer,
    ) -> u64 {
        let nsrc = sources.len();
        let dst_rowid = vmap.local_to_rowid(dst);
        let mut remaining_bits = bits;
        while remaining_bits != 0 {
            if buffer.is_full() {
                return remaining_bits;
            }
            let lane = remaining_bits.trailing_zeros() as usize;
            if lane >= nsrc {
                remaining_bits &= remaining_bits - 1;
                continue;
            }
            let mut row = sources[lane].0.clone();
            row.push(edge_rowid);
            row.push(dst as u64);
            row.push(dst_rowid);
            remaining_bits &= remaining_bits - 1;
            let _ = buffer.push_row(row, None);
        }
        0
    }

    fn advance_lane_state(
        &self,
        mut state: LaneBfsState,
        snapshot: &GraphReadSnapshot,
        edge_label: &str,
        vmap: &VertexIdMap,
        buffer: &mut GraphPathOutputBuffer,
        forward_neighbor_scratch: &mut Vec<(u32, u64)>,
        backward_neighbor_scratch: &mut Vec<(u32, u64)>,
        temporary_memory_state: Option<&Arc<TemporaryMemoryState>>,
    ) -> LaneAdvanceResult {
        if let Some(temp_state) = temporary_memory_state {
            self.update_lane_temporary_memory(temp_state, &state);
        }
        while !buffer.is_full() {
            if let Some(pending) = state.pending_emit.take() {
                let remaining_bits = self.emit_lane_rows(
                    &state.sources,
                    vmap,
                    pending.remaining_bits,
                    pending.edge_rowid,
                    pending.dst,
                    buffer,
                );
                if remaining_bits != 0 {
                    state.pending_emit = Some(PendingLaneEmission {
                        remaining_bits,
                        edge_rowid: pending.edge_rowid,
                        dst: pending.dst,
                    });
                    return LaneAdvanceResult::Suspended(state);
                }
            }

            if state.current_hop > self.max_hops || state.frontier.is_empty() {
                return LaneAdvanceResult::Finished {
                    seen: state.seen,
                    visit: state.visit,
                    visit_next: state.visit_next,
                };
            }

            if state.frontier_idx >= state.frontier.len() {
                for &v_local in &state.frontier {
                    state.visit[v_local as usize] = 0;
                }
                if state.next_frontier.is_empty() {
                    return LaneAdvanceResult::Finished {
                        seen: state.seen,
                        visit: state.visit,
                        visit_next: state.visit_next,
                    };
                }
                storage_metrics().set_graph_frontier_size(state.next_frontier.len());
                for &v_local in &state.next_frontier {
                    let idx = v_local as usize;
                    let next_bits = state.visit_next[idx];
                    state.seen[idx] |= next_bits;
                    state.visit[idx] = next_bits;
                    state.visit_next[idx] = 0;
                }
                state.frontier = std::mem::take(&mut state.next_frontier);
                state.frontier_idx = 0;
                state.neighbor_direction = 0;
                state.neighbor_idx = 0;
                state.current_hop += 1;
                if let Some(temp_state) = temporary_memory_state {
                    self.update_lane_temporary_memory(temp_state, &state);
                }
                continue;
            }

            let v_local = state.frontier[state.frontier_idx];
            let lane_mask = state.visit[v_local as usize];
            if lane_mask == 0 {
                state.frontier_idx += 1;
                state.neighbor_direction = 0;
                state.neighbor_idx = 0;
                continue;
            }

            let view = match state.neighbor_direction {
                0 if self.direction == ExpandDirection::Forward
                    || self.direction == ExpandDirection::Both =>
                {
                    snapshot.neighbors_forward(edge_label, v_local, forward_neighbor_scratch)
                }
                0 => {
                    state.neighbor_direction = 1;
                    state.neighbor_idx = 0;
                    continue;
                }
                1 if self.direction == ExpandDirection::Backward
                    || self.direction == ExpandDirection::Both =>
                {
                    snapshot.neighbors_backward(edge_label, v_local, backward_neighbor_scratch)
                }
                1 => {
                    state.frontier_idx += 1;
                    state.neighbor_direction = 0;
                    state.neighbor_idx = 0;
                    continue;
                }
                _ => {
                    state.frontier_idx += 1;
                    state.neighbor_direction = 0;
                    state.neighbor_idx = 0;
                    continue;
                }
            };

            let Some(view) = view else {
                state.neighbor_direction += 1;
                state.neighbor_idx = 0;
                continue;
            };

            if state.neighbor_idx >= view.len() {
                state.neighbor_direction += 1;
                state.neighbor_idx = 0;
                continue;
            }

            let Some((dst, edge_rowid)) = view.pair_at(state.neighbor_idx) else {
                state.neighbor_idx += 1;
                continue;
            };
            state.neighbor_idx += 1;

            let dst_idx = dst as usize;
            let new_bits = lane_mask & !state.seen[dst_idx];
            if new_bits == 0 {
                continue;
            }
            let prev_next = state.visit_next[dst_idx];
            state.visit_next[dst_idx] = prev_next | new_bits;
            if prev_next == 0 {
                state.next_frontier.push(dst);
            }

            if state.current_hop < self.min_hops {
                continue;
            }

            let emit_bits = match self.path_mode {
                PathMode::AllShortest => new_bits,
                PathMode::AnyShortest | PathMode::Any | PathMode::All => new_bits & !prev_next,
            };
            if emit_bits == 0 {
                continue;
            }

            let remaining_bits =
                self.emit_lane_rows(&state.sources, vmap, emit_bits, edge_rowid, dst, buffer);
            if remaining_bits != 0 {
                state.pending_emit = Some(PendingLaneEmission {
                    remaining_bits,
                    edge_rowid,
                    dst,
                });
                return LaneAdvanceResult::Suspended(state);
            }
        }
        LaneAdvanceResult::Suspended(state)
    }

    fn materialize_path_from_parent_chain(
        &self,
        input: &Chunk,
        row: usize,
        parents: &SpillableParentArrays,
        lookup_state: &mut ParentLookupState,
        terminal_parent: u32,
        terminal_edge: u64,
        dst: u32,
        hop: u64,
        vmap: &VertexIdMap,
    ) -> Result<MaterializedPath> {
        let mut path = collect_prefix_path(input, row, &self.path_emit_spec);
        let mut segment_vertices = vec![terminal_parent];
        let mut segment_edges = Vec::with_capacity(hop as usize);
        let mut current = terminal_parent;

        for hop_idx in (0..hop.saturating_sub(1) as usize).rev() {
            let (parent_v, parent_e) =
                parents.lookup_parent(hop_idx, current as usize, lookup_state)?;
            if parent_v == u32::MAX {
                break;
            }
            segment_edges.push(parent_e);
            segment_vertices.push(parent_v);
            current = parent_v;
        }

        segment_vertices.reverse();
        segment_edges.reverse();
        segment_edges.push(terminal_edge);

        for edge_rowid in segment_edges {
            path.edges.push(PathElementRef {
                table_oid: self.path_emit_spec.segment_edge_table_oid,
                rowid: edge_rowid,
            });
        }
        for &vertex_local in segment_vertices.iter().skip(1) {
            path.vertices.push(PathElementRef {
                table_oid: self.path_emit_spec.segment_vertex_table_oid,
                rowid: vmap.local_to_rowid(vertex_local),
            });
        }
        path.vertices.push(PathElementRef {
            table_oid: self.path_emit_spec.segment_vertex_table_oid,
            rowid: vmap.local_to_rowid(dst),
        });
        path.length += hop as i64;
        Ok(path)
    }

    fn frontier_degree(
        &self,
        frontier: &SpillableFrontier,
        snapshot: &GraphReadSnapshot,
        edge_label: &str,
        toward_target: bool,
        forward_neighbor_scratch: &mut Vec<(u32, u64)>,
        backward_neighbor_scratch: &mut Vec<(u32, u64)>,
    ) -> Result<usize> {
        let mut total = 0usize;
        let mut cursor = SpillableFrontierCursor::default();
        for idx in 0..frontier.len() {
            let local_id = frontier.get(idx, &mut cursor)?;
            total += match (toward_target, self.direction) {
                (true, ExpandDirection::Forward) => snapshot
                    .neighbors_forward(edge_label, local_id, forward_neighbor_scratch)
                    .map(|view| view.len())
                    .unwrap_or(0),
                (true, ExpandDirection::Backward) => snapshot
                    .neighbors_backward(edge_label, local_id, backward_neighbor_scratch)
                    .map(|view| view.len())
                    .unwrap_or(0),
                (true, ExpandDirection::Both) => {
                    let forward = snapshot
                        .neighbors_forward(edge_label, local_id, forward_neighbor_scratch)
                        .map(|view| view.len())
                        .unwrap_or(0);
                    let backward = snapshot
                        .neighbors_backward(edge_label, local_id, backward_neighbor_scratch)
                        .map(|view| view.len())
                        .unwrap_or(0);
                    forward + backward
                }
                (false, ExpandDirection::Forward) => snapshot
                    .neighbors_backward(edge_label, local_id, backward_neighbor_scratch)
                    .map(|view| view.len())
                    .unwrap_or(0),
                (false, ExpandDirection::Backward) => snapshot
                    .neighbors_forward(edge_label, local_id, forward_neighbor_scratch)
                    .map(|view| view.len())
                    .unwrap_or(0),
                (false, ExpandDirection::Both) => {
                    let backward = snapshot
                        .neighbors_backward(edge_label, local_id, backward_neighbor_scratch)
                        .map(|view| view.len())
                        .unwrap_or(0);
                    let forward = snapshot
                        .neighbors_forward(edge_label, local_id, forward_neighbor_scratch)
                        .map(|view| view.len())
                        .unwrap_or(0);
                    backward + forward
                }
            };
        }
        Ok(total)
    }

    fn for_each_bound_target_neighbor<F>(
        &self,
        snapshot: &GraphReadSnapshot,
        edge_label: &str,
        local_id: u32,
        toward_target: bool,
        forward_neighbor_scratch: &mut Vec<(u32, u64)>,
        backward_neighbor_scratch: &mut Vec<(u32, u64)>,
        mut callback: F,
    ) -> Result<()>
    where
        F: FnMut(u32, u64) -> Result<()>,
    {
        match (toward_target, self.direction) {
            (true, ExpandDirection::Forward) => {
                if let Some(view) =
                    snapshot.neighbors_forward(edge_label, local_id, forward_neighbor_scratch)
                {
                    for idx in 0..view.len() {
                        if let Some((neighbor, edge_rowid)) = view.pair_at(idx) {
                            callback(neighbor, edge_rowid)?;
                        }
                    }
                }
            }
            (true, ExpandDirection::Backward) => {
                if let Some(view) =
                    snapshot.neighbors_backward(edge_label, local_id, backward_neighbor_scratch)
                {
                    for idx in 0..view.len() {
                        if let Some((neighbor, edge_rowid)) = view.pair_at(idx) {
                            callback(neighbor, edge_rowid)?;
                        }
                    }
                }
            }
            (true, ExpandDirection::Both) => {
                if let Some(view) =
                    snapshot.neighbors_forward(edge_label, local_id, forward_neighbor_scratch)
                {
                    for idx in 0..view.len() {
                        if let Some((neighbor, edge_rowid)) = view.pair_at(idx) {
                            callback(neighbor, edge_rowid)?;
                        }
                    }
                }
                if let Some(view) =
                    snapshot.neighbors_backward(edge_label, local_id, backward_neighbor_scratch)
                {
                    for idx in 0..view.len() {
                        if let Some((neighbor, edge_rowid)) = view.pair_at(idx) {
                            callback(neighbor, edge_rowid)?;
                        }
                    }
                }
            }
            (false, ExpandDirection::Forward) => {
                if let Some(view) =
                    snapshot.neighbors_backward(edge_label, local_id, backward_neighbor_scratch)
                {
                    for idx in 0..view.len() {
                        if let Some((neighbor, edge_rowid)) = view.pair_at(idx) {
                            callback(neighbor, edge_rowid)?;
                        }
                    }
                }
            }
            (false, ExpandDirection::Backward) => {
                if let Some(view) =
                    snapshot.neighbors_forward(edge_label, local_id, forward_neighbor_scratch)
                {
                    for idx in 0..view.len() {
                        if let Some((neighbor, edge_rowid)) = view.pair_at(idx) {
                            callback(neighbor, edge_rowid)?;
                        }
                    }
                }
            }
            (false, ExpandDirection::Both) => {
                if let Some(view) =
                    snapshot.neighbors_backward(edge_label, local_id, backward_neighbor_scratch)
                {
                    for idx in 0..view.len() {
                        if let Some((neighbor, edge_rowid)) = view.pair_at(idx) {
                            callback(neighbor, edge_rowid)?;
                        }
                    }
                }
                if let Some(view) =
                    snapshot.neighbors_forward(edge_label, local_id, forward_neighbor_scratch)
                {
                    for idx in 0..view.len() {
                        if let Some((neighbor, edge_rowid)) = view.pair_at(idx) {
                            callback(neighbor, edge_rowid)?;
                        }
                    }
                }
            }
        }
        Ok(())
    }

    fn reconstruct_bound_target_segment(
        &self,
        src: u32,
        dst: u32,
        meet: u32,
        from_src: &BidirectionalDirectionState,
        from_dst: &BidirectionalDirectionState,
        src_lookup_state: &mut ParentLookupState,
        dst_lookup_state: &mut ParentLookupState,
    ) -> Result<Option<(Vec<u32>, Vec<u64>)>> {
        let src_parents = from_src.parents.as_ref().ok_or_else(|| {
            paro_error::internal("Missing source-side parent state for bound-target path")
        })?;
        let dst_parents = from_dst.parents.as_ref().ok_or_else(|| {
            paro_error::internal("Missing target-side parent state for bound-target path")
        })?;
        let mut left_vertices = vec![meet];
        let mut left_edges = Vec::new();
        let mut current = meet;
        let src_depth = *from_src
            .visited_depth
            .get(meet as usize)
            .ok_or_else(|| paro_error::internal("Invalid meet vertex source depth"))?;
        for hop_idx in (0..src_depth as usize).rev() {
            let (parent, edge) =
                src_parents.lookup_parent(hop_idx, current as usize, src_lookup_state)?;
            if parent == u32::MAX {
                return Ok(None);
            }
            left_edges.push(edge);
            left_vertices.push(parent);
            current = parent;
        }
        left_vertices.reverse();
        left_edges.reverse();

        let mut right_vertices = Vec::new();
        let mut right_edges = Vec::new();
        current = meet;
        let dst_depth = *from_dst
            .visited_depth
            .get(meet as usize)
            .ok_or_else(|| paro_error::internal("Invalid meet vertex target depth"))?;
        for hop_idx in (0..dst_depth as usize).rev() {
            let (next, edge) =
                dst_parents.lookup_parent(hop_idx, current as usize, dst_lookup_state)?;
            if next == u32::MAX {
                return Ok(None);
            }
            right_edges.push(edge);
            right_vertices.push(next);
            current = next;
        }

        let mut vertices = left_vertices;
        vertices.extend(right_vertices);
        let mut edges = left_edges;
        edges.extend(right_edges);
        debug_assert_eq!(vertices.first().copied(), Some(src));
        debug_assert_eq!(vertices.last().copied(), Some(dst));
        Ok(Some((vertices, edges)))
    }

    fn materialize_bound_target_path(
        &self,
        input: &Chunk,
        row: usize,
        vertices: &[u32],
        edges: &[u64],
        vmap: &VertexIdMap,
    ) -> MaterializedPath {
        let mut path = collect_prefix_path(input, row, &self.path_emit_spec);
        for &edge_rowid in edges {
            path.edges.push(PathElementRef {
                table_oid: self.path_emit_spec.segment_edge_table_oid,
                rowid: edge_rowid,
            });
        }
        for &vertex_local in vertices.iter().skip(1) {
            path.vertices.push(PathElementRef {
                table_oid: self.path_emit_spec.segment_vertex_table_oid,
                rowid: vmap.local_to_rowid(vertex_local),
            });
        }
        path.length += edges.len() as i64;
        path
    }

    fn evaluate_target_filter(
        &self,
        ctx: &ExecutionContext,
        index: &GraphProjectionIndex,
    ) -> Result<Vec<u64>> {
        let filter = self.target_filter.as_ref().unwrap();

        let vertex_map = index.vertex_map(&self.target_label).ok_or_else(|| {
            paro_error::internal(format!(
                "Vertex map for label \"{}\" not found in graph \"{}\"",
                self.target_label, self.graph_name
            ))
        })?;

        let num_vertices = vertex_map.num_vertices() as usize;
        let mut bitset = vec![0u64; num_vertices.div_ceil(64)];

        let target_table = if self.direction == ExpandDirection::Backward {
            &self.edge_info.source_vertex_table
        } else {
            &self.edge_info.destination_vertex_table
        };

        let table_name = if !self.target_table_name.is_empty() {
            &self.target_table_name
        } else {
            target_table
        };
        let schema = if !self.schema_name.is_empty() {
            &self.schema_name
        } else {
            "public"
        };

        let catalog = ctx.catalog();
        let txn = ctx.catalog_txn_view();
        let visible_version = i64::try_from(ctx.transaction_visible_version()).unwrap_or(i64::MAX);

        let table_entry = catalog.get_table(&txn, schema, table_name)?;
        let table = match table_entry.as_ref() {
            CatalogEntryEnum::Table(t) => t,
            _ => return Err(paro_error::wrong_object_type("table", table_name)),
        };
        let storage = table.get_storage().ok_or_else(|| {
            paro_error::internal(format!(
                "Target vertex table \"{}\" has no storage",
                table_name
            ))
        })?;

        let filter_col_ids = extract_graph_filter_column_ids(filter);
        let params = TabletReaderParams::with_version(visible_version)
            .with_columns(filter_col_ids.clone())
            .with_emit_row_id(true);
        let mut reader = storage.create_reader(params)?;
        reader.prepare()?;

        let remapped_filter = remap_graph_filter_columns(filter, &filter_col_ids);
        let filter_exprs = vec![remapped_filter];

        while let Some(scan_chunk) = reader.get_next_chunk()? {
            let scan_size = scan_chunk.size();
            if scan_size == 0 {
                continue;
            }

            let rowid_col_idx = scan_chunk.column_count() - 1;
            let mut filter_executor = ExpressionExecutor::with_expressions(&filter_exprs);
            let mut filter_result = Chunk::initialize(&[LogicalType::Boolean], scan_size);
            filter_executor.execute_all_into(&scan_chunk, ctx, &mut filter_result)?;

            let bool_col = filter_result.column(0).ok_or_else(|| {
                paro_error::internal("Missing boolean column from filter evaluation")
            })?;
            let rowid_col = scan_chunk.column(rowid_col_idx).ok_or_else(|| {
                paro_error::internal("Missing rowid column in target vertex scan")
            })?;

            for row in 0..scan_size {
                if bool_col.get_bool(row).unwrap_or(false) {
                    let rowid = rowid_col.get_i64(row).unwrap_or(0) as u64;
                    if let Some(local_id) = vertex_map.rowid_to_local(rowid) {
                        graph_bitset_set(&mut bitset, local_id);
                    }
                }
            }
        }

        Ok(bitset)
    }

    fn advance_bidirectional_direction(
        &self,
        state: &mut BidirectionalDirectionState,
        other: &BidirectionalDirectionState,
        snapshot: &GraphReadSnapshot,
        edge_label: &str,
        toward_target: bool,
        forward_neighbor_scratch: &mut Vec<(u32, u64)>,
        backward_neighbor_scratch: &mut Vec<(u32, u64)>,
    ) -> Result<Option<BoundTargetSearchResult>> {
        let next_depth = state.current_hop;
        let mut best_result: Option<BoundTargetSearchResult> = None;

        for idx in 0..state.frontier.len() {
            let local_id = state.frontier.get(idx, &mut state.frontier_cursor)?;
            self.for_each_bound_target_neighbor(
                snapshot,
                edge_label,
                local_id,
                toward_target,
                forward_neighbor_scratch,
                backward_neighbor_scratch,
                |neighbor, edge_rowid| {
                    let neighbor_idx = neighbor as usize;
                    let candidate_terminal_edge = if state.visited_depth[local_id as usize] == 0 {
                        edge_rowid
                    } else {
                        state.terminal_edge_to_root[local_id as usize]
                    };
                    if !graph_bitset_test(&state.seen, neighbor) {
                        graph_bitset_set(&mut state.seen, neighbor);
                        state.visited_depth[neighbor_idx] = next_depth;
                        state.next_frontier.push(neighbor)?;
                        state.terminal_edge_to_root[neighbor_idx] = candidate_terminal_edge;
                        if let Some(parents) = state.parents.as_mut() {
                            parents.set_parent(neighbor, local_id, edge_rowid);
                        }
                    }

                    if !graph_bitset_test(&other.seen, neighbor) {
                        return Ok(());
                    }

                    let total = next_depth as u64 + other.visited_depth[neighbor_idx] as u64;
                    if total < self.min_hops || total > self.max_hops {
                        return Ok(());
                    }

                    let terminal_edge_rowid = if toward_target {
                        if other.visited_depth[neighbor_idx] == 0 {
                            edge_rowid
                        } else {
                            other.terminal_edge_to_root[neighbor_idx]
                        }
                    } else {
                        candidate_terminal_edge
                    };
                    let should_replace = best_result
                        .as_ref()
                        .map(|best| total < best.path_len)
                        .unwrap_or(true);
                    if should_replace {
                        best_result = Some(BoundTargetSearchResult {
                            meet: neighbor,
                            path_len: total,
                            terminal_edge_rowid,
                        });
                    }
                    Ok(())
                },
            )?;
        }

        if !state.next_frontier.is_empty() {
            if let Some(parents) = state.parents.as_mut() {
                parents.commit_hop()?;
            }
        }
        state.frontier = state.next_frontier.take();
        state.frontier_cursor = SpillableFrontierCursor::default();
        state.current_hop += 1;
        Ok(best_result)
    }

    fn bfs_shortest_between_bound_vertices(
        &self,
        input: &Chunk,
        row: usize,
        src_local: u32,
        dst_local: u32,
        snapshot: &GraphReadSnapshot,
        edge_label: &str,
        vmap: &VertexIdMap,
        output_buffer: &mut GraphPathOutputBuffer,
        forward_neighbor_scratch: &mut Vec<(u32, u64)>,
        backward_neighbor_scratch: &mut Vec<(u32, u64)>,
        buffer_pool: Arc<paro_storage::buffer::BufferPool>,
        frontier_threshold: usize,
        temporary_memory_state: Option<&Arc<TemporaryMemoryState>>,
    ) -> Result<()> {
        if src_local == dst_local {
            return Ok(());
        }

        let nv = vmap.num_vertices() as usize;
        if nv == 0 {
            return Ok(());
        }

        let vals = Self::collect_input_row_values(input, row);
        let word_count = nv.div_ceil(64);
        let mut src_frontier = SpillableFrontier::new(buffer_pool.clone(), frontier_threshold);
        src_frontier.push(src_local)?;
        let mut dst_frontier = SpillableFrontier::new(buffer_pool.clone(), frontier_threshold);
        dst_frontier.push(dst_local)?;

        let create_parents = self.emit_path_info;
        let mut from_src = BidirectionalDirectionState {
            current_hop: 1,
            frontier: src_frontier,
            next_frontier: SpillableFrontier::new(buffer_pool.clone(), frontier_threshold),
            frontier_cursor: SpillableFrontierCursor::default(),
            seen: vec![0u64; word_count],
            visited_depth: vec![u32::MAX; nv],
            terminal_edge_to_root: vec![0u64; nv],
            parents: create_parents.then(|| SpillableParentArrays::new(nv, buffer_pool.clone())),
        };
        graph_bitset_set(&mut from_src.seen, src_local);
        from_src.visited_depth[src_local as usize] = 0;

        let mut from_dst = BidirectionalDirectionState {
            current_hop: 1,
            frontier: dst_frontier,
            next_frontier: SpillableFrontier::new(buffer_pool.clone(), frontier_threshold),
            frontier_cursor: SpillableFrontierCursor::default(),
            seen: vec![0u64; word_count],
            visited_depth: vec![u32::MAX; nv],
            terminal_edge_to_root: vec![0u64; nv],
            parents: create_parents.then(|| SpillableParentArrays::new(nv, buffer_pool)),
        };
        graph_bitset_set(&mut from_dst.seen, dst_local);
        from_dst.visited_depth[dst_local as usize] = 0;

        let mut best_result: Option<BoundTargetSearchResult> = None;
        if let Some(temp_state) = temporary_memory_state {
            self.update_bound_target_temporary_memory(temp_state, &mut from_src, &mut from_dst)?;
        }

        while !from_src.frontier.is_empty() && !from_dst.frontier.is_empty() {
            storage_metrics()
                .set_graph_frontier_size(from_src.frontier.len() + from_dst.frontier.len());
            let lower_bound = (from_src.current_hop.saturating_sub(1)
                + from_dst.current_hop.saturating_sub(1)) as u64;
            if lower_bound >= self.max_hops
                || best_result
                    .as_ref()
                    .is_some_and(|result| lower_bound >= result.path_len)
            {
                break;
            }

            let expand_from_src = self.frontier_degree(
                &from_src.frontier,
                snapshot,
                edge_label,
                true,
                forward_neighbor_scratch,
                backward_neighbor_scratch,
            )? <= self.frontier_degree(
                &from_dst.frontier,
                snapshot,
                edge_label,
                false,
                forward_neighbor_scratch,
                backward_neighbor_scratch,
            )?;

            let next_result = if expand_from_src {
                self.advance_bidirectional_direction(
                    &mut from_src,
                    &from_dst,
                    snapshot,
                    edge_label,
                    true,
                    forward_neighbor_scratch,
                    backward_neighbor_scratch,
                )?
            } else {
                self.advance_bidirectional_direction(
                    &mut from_dst,
                    &from_src,
                    snapshot,
                    edge_label,
                    false,
                    forward_neighbor_scratch,
                    backward_neighbor_scratch,
                )?
            };

            if let Some(candidate) = next_result {
                let should_replace = best_result
                    .as_ref()
                    .map(|best| candidate.path_len < best.path_len)
                    .unwrap_or(true);
                if should_replace {
                    best_result = Some(candidate);
                }
            }
            if let Some(temp_state) = temporary_memory_state {
                self.update_bound_target_temporary_memory(
                    temp_state,
                    &mut from_src,
                    &mut from_dst,
                )?;
            }
        }

        let Some(best_result) = best_result else {
            if let Some(temp_state) = temporary_memory_state {
                temp_state.set_zero();
            }
            return Ok(());
        };

        let mut out_row = vals;
        out_row.push(best_result.terminal_edge_rowid);
        out_row.push(dst_local as u64);
        out_row.push(vmap.local_to_rowid(dst_local));
        let path = if self.emit_path_info {
            let mut src_lookup_state = ParentLookupState::new();
            let mut dst_lookup_state = ParentLookupState::new();
            let Some((vertices, edges)) = self.reconstruct_bound_target_segment(
                src_local,
                dst_local,
                best_result.meet,
                &from_src,
                &from_dst,
                &mut src_lookup_state,
                &mut dst_lookup_state,
            )?
            else {
                if let Some(temp_state) = temporary_memory_state {
                    temp_state.set_zero();
                }
                return Ok(());
            };
            if edges.is_empty() {
                if let Some(temp_state) = temporary_memory_state {
                    temp_state.set_zero();
                }
                return Ok(());
            }
            Some(self.materialize_bound_target_path(input, row, &vertices, &edges, vmap))
        } else {
            None
        };
        let _ = output_buffer.push_row(out_row, path);
        if let Some(temp_state) = temporary_memory_state {
            temp_state.set_zero();
        }
        Ok(())
    }

    fn init_single_source_state(
        &self,
        input: &Chunk,
        row: usize,
        src_local: u32,
        vmap: &VertexIdMap,
        buffer_pool: Arc<paro_storage::buffer::BufferPool>,
        frontier_threshold: usize,
    ) -> Result<Option<SingleSourceBfsState>> {
        let nv = vmap.num_vertices() as usize;
        if nv == 0 {
            return Ok(None);
        }

        let mut seen = vec![false; nv];
        let mut frontier = SpillableFrontier::new(buffer_pool.clone(), frontier_threshold);
        frontier.push(src_local)?;
        seen[src_local as usize] = true;

        Ok(Some(SingleSourceBfsState {
            row,
            input_vals: Self::collect_input_row_values(input, row),
            seen,
            next_seen: vec![false; nv],
            frontier,
            frontier_cursor: SpillableFrontierCursor::default(),
            next_frontier: SpillableFrontier::new(buffer_pool.clone(), frontier_threshold),
            parents: SpillableParentArrays::new(nv, buffer_pool),
            parent_lookup_state: ParentLookupState::new(),
            frontier_idx: 0,
            neighbor_direction: 0,
            neighbor_idx: 0,
            current_hop: 1,
        }))
    }

    fn advance_single_source_state(
        &self,
        input: &Chunk,
        mut state: SingleSourceBfsState,
        snapshot: &GraphReadSnapshot,
        edge_label: &str,
        vmap: &VertexIdMap,
        output_buffer: &mut GraphPathOutputBuffer,
        forward_neighbor_scratch: &mut Vec<(u32, u64)>,
        backward_neighbor_scratch: &mut Vec<(u32, u64)>,
        temporary_memory_state: Option<&Arc<TemporaryMemoryState>>,
    ) -> Result<Option<SingleSourceBfsState>> {
        if let Some(temp_state) = temporary_memory_state {
            self.update_single_source_temporary_memory(temp_state, &mut state)?;
        }
        while !output_buffer.is_full() {
            if state.current_hop > self.max_hops || state.frontier.is_empty() {
                return Ok(None);
            }

            if state.frontier_idx >= state.frontier.len() {
                if state.next_frontier.is_empty() {
                    return Ok(None);
                }
                storage_metrics().set_graph_frontier_size(state.next_frontier.len());
                let mut next_frontier_cursor = SpillableFrontierCursor::default();
                for idx in 0..state.next_frontier.len() {
                    let v_local = state.next_frontier.get(idx, &mut next_frontier_cursor)?;
                    let idx = v_local as usize;
                    state.seen[idx] = true;
                    state.next_seen[idx] = false;
                }
                state.parents.commit_hop()?;
                state.frontier = state.next_frontier.take();
                state.frontier_cursor = SpillableFrontierCursor::default();
                state.frontier_idx = 0;
                state.neighbor_direction = 0;
                state.neighbor_idx = 0;
                state.current_hop += 1;
                if state.current_hop > self.max_hops {
                    return Ok(None);
                }
                if let Some(temp_state) = temporary_memory_state {
                    self.update_single_source_temporary_memory(temp_state, &mut state)?;
                }
                continue;
            }

            let v_local = state
                .frontier
                .get(state.frontier_idx, &mut state.frontier_cursor)?;
            let view = match state.neighbor_direction {
                0 if self.direction == ExpandDirection::Forward
                    || self.direction == ExpandDirection::Both =>
                {
                    snapshot.neighbors_forward(edge_label, v_local, forward_neighbor_scratch)
                }
                0 => {
                    state.neighbor_direction = 1;
                    state.neighbor_idx = 0;
                    continue;
                }
                1 if self.direction == ExpandDirection::Backward
                    || self.direction == ExpandDirection::Both =>
                {
                    snapshot.neighbors_backward(edge_label, v_local, backward_neighbor_scratch)
                }
                1 => {
                    state.frontier_idx += 1;
                    state.neighbor_direction = 0;
                    state.neighbor_idx = 0;
                    continue;
                }
                _ => {
                    state.frontier_idx += 1;
                    state.neighbor_direction = 0;
                    state.neighbor_idx = 0;
                    continue;
                }
            };

            let Some(view) = view else {
                state.neighbor_direction += 1;
                state.neighbor_idx = 0;
                continue;
            };

            if state.neighbor_idx >= view.len() {
                state.neighbor_direction += 1;
                state.neighbor_idx = 0;
                continue;
            }

            let Some((dst, edge_rowid)) = view.pair_at(state.neighbor_idx) else {
                state.neighbor_idx += 1;
                continue;
            };
            state.neighbor_idx += 1;

            if state.seen[dst as usize] {
                continue;
            }

            let already_in_next = state.next_seen[dst as usize];
            if !already_in_next {
                state.next_seen[dst as usize] = true;
                state.next_frontier.push(dst)?;
                state.parents.set_parent(dst, v_local, edge_rowid);
            }

            if state.current_hop < self.min_hops {
                continue;
            }

            let should_emit =
                matches!(self.path_mode, PathMode::AllShortest | PathMode::All) || !already_in_next;
            if !should_emit {
                continue;
            }

            let mut out_row = state.input_vals.clone();
            out_row.push(edge_rowid);
            out_row.push(dst as u64);
            out_row.push(vmap.local_to_rowid(dst));
            let path = self.materialize_path_from_parent_chain(
                input,
                state.row,
                &state.parents,
                &mut state.parent_lookup_state,
                v_local,
                edge_rowid,
                dst,
                state.current_hop,
                vmap,
            )?;
            let action = output_buffer.push_row(out_row, Some(path));
            if action == EmitAction::Yield {
                if let Some(temp_state) = temporary_memory_state {
                    self.update_single_source_temporary_memory(temp_state, &mut state)?;
                }
                return Ok(Some(state));
            }
        }
        if let Some(temp_state) = temporary_memory_state {
            self.update_single_source_temporary_memory(temp_state, &mut state)?;
        }
        Ok(Some(state))
    }

    fn materialize_output_chunk(
        &self,
        chunk: &mut Chunk,
        ncols: usize,
        output_rows: &[Vec<u64>],
        path_rows: &[MaterializedPath],
    ) {
        let count = output_rows.len();
        let ocols = self.output_types.len();
        let mut vectors: Vec<Vector> = self
            .output_types
            .iter()
            .map(|t| {
                let mut v = Vector::with_capacity(t.clone(), count.max(VECTOR_SIZE));
                v.set_len(count);
                v
            })
            .collect();
        let path_base = ncols + 3;
        for (ri, row) in output_rows.iter().enumerate() {
            for (ci, &val) in row.iter().enumerate() {
                if ci < ocols {
                    match &self.output_types[ci] {
                        LogicalType::BigInt => vectors[ci].set_i64(ri, val as i64),
                        _ => vectors[ci].set_u64(ri, val),
                    }
                }
            }
        }
        let path_vectors = if self.emit_path_info {
            Some(materialize_path_vectors(path_rows))
        } else {
            None
        };
        let mut arcs: Vec<Arc<Vector>> = Vec::with_capacity(ocols);
        for (idx, v) in vectors.into_iter().enumerate() {
            if let Some((path_len, vertices, edges)) = &path_vectors {
                if idx == path_base + PATH_LENGTH_OFFSET {
                    arcs.push(path_len.clone());
                    continue;
                }
                if idx == path_base + PATH_VERTICES_OFFSET {
                    arcs.push(vertices.clone());
                    continue;
                }
                if idx == path_base + PATH_EDGES_OFFSET {
                    arcs.push(edges.clone());
                    continue;
                }
            }
            arcs.push(Arc::new(v));
        }
        *chunk = Chunk::from_arc_vectors(arcs);
        chunk.set_cardinality(count);
    }
}

#[inline]
fn single_local_from_bitset(bitset: &[u64]) -> Option<u32> {
    let mut found = None;
    for (word_idx, word) in bitset.iter().copied().enumerate() {
        if word == 0 {
            continue;
        }
        if word.count_ones() > 1 || found.is_some() {
            return None;
        }
        let bit_idx = word.trailing_zeros() as usize;
        found = Some((word_idx * 64 + bit_idx) as u32);
    }
    found
}

#[inline]
fn graph_bitset_set(bitset: &mut [u64], local_id: u32) {
    let word_idx = (local_id / 64) as usize;
    let bit_idx = local_id % 64;
    if word_idx < bitset.len() {
        bitset[word_idx] |= 1u64 << bit_idx;
    }
}

#[inline]
fn graph_bitset_test(bitset: &[u64], local_id: u32) -> bool {
    let word_idx = (local_id / 64) as usize;
    let bit_idx = local_id % 64;
    word_idx < bitset.len() && (bitset[word_idx] & (1u64 << bit_idx)) != 0
}

fn extract_graph_filter_column_ids(expr: &Expression) -> Vec<usize> {
    let mut ids = Vec::new();
    collect_graph_filter_column_ids(expr, &mut ids);
    ids.sort();
    ids.dedup();
    ids
}

fn collect_graph_filter_column_ids(expr: &Expression, ids: &mut Vec<usize>) {
    match expr {
        Expression::ColumnRef(col_ref) => ids.push(col_ref.binding.column_index),
        Expression::Comparison(cmp) => {
            collect_graph_filter_column_ids(&cmp.left, ids);
            collect_graph_filter_column_ids(&cmp.right, ids);
        }
        Expression::Conjunction(conj) => {
            for child in &conj.children {
                collect_graph_filter_column_ids(child, ids);
            }
        }
        Expression::Function(func) => {
            for child in &func.children {
                collect_graph_filter_column_ids(child, ids);
            }
        }
        Expression::Cast(cast) => collect_graph_filter_column_ids(&cast.child, ids),
        Expression::Operator(op) => {
            for child in &op.children {
                collect_graph_filter_column_ids(child, ids);
            }
        }
        _ => {}
    }
}

fn remap_graph_filter_columns(expr: &Expression, col_ids: &[usize]) -> Expression {
    use paro_planner::expression::ColumnRefExpression;
    use paro_planner::operator::ColumnBinding;

    match expr {
        Expression::ColumnRef(col_ref) => {
            let original_col = col_ref.binding.column_index;
            let new_col = col_ids
                .iter()
                .position(|&id| id == original_col)
                .unwrap_or(0);
            Expression::ColumnRef(ColumnRefExpression::new(
                ColumnBinding::new(col_ref.binding.table_index, new_col),
                col_ref.return_type.clone(),
            ))
        }
        Expression::Comparison(cmp) => {
            let mut new_cmp = cmp.clone();
            new_cmp.left = Box::new(remap_graph_filter_columns(&cmp.left, col_ids));
            new_cmp.right = Box::new(remap_graph_filter_columns(&cmp.right, col_ids));
            Expression::Comparison(new_cmp)
        }
        Expression::Conjunction(conj) => {
            let mut new_conj = conj.clone();
            new_conj.children = conj
                .children
                .iter()
                .map(|child| remap_graph_filter_columns(child, col_ids))
                .collect();
            Expression::Conjunction(new_conj)
        }
        Expression::Function(func) => {
            let mut new_func = func.clone();
            new_func.children = func
                .children
                .iter()
                .map(|child| remap_graph_filter_columns(child, col_ids))
                .collect();
            Expression::Function(new_func)
        }
        Expression::Cast(cast) => {
            let mut new_cast = cast.clone();
            new_cast.child = Box::new(remap_graph_filter_columns(&cast.child, col_ids));
            Expression::Cast(new_cast)
        }
        Expression::Operator(op) => {
            let mut new_op = op.clone();
            new_op.children = op
                .children
                .iter()
                .map(|child| remap_graph_filter_columns(child, col_ids))
                .collect();
            Expression::Operator(new_op)
        }
        other => other.clone(),
    }
}

impl PhysicalOperator for PhysicalGraphShortestPath {
    fn operator_type(&self) -> PhysicalOperatorType {
        PhysicalOperatorType::GraphShortestPath
    }

    fn types(&self) -> &[LogicalType] {
        &self.output_types
    }

    fn explain_params(&self) -> Vec<String> {
        let dir = match self.direction {
            ExpandDirection::Forward => "Forward",
            ExpandDirection::Backward => "Backward",
            ExpandDirection::Both => "Both",
        };
        let mut params = vec![
            format!("Graph: {}", self.graph_name),
            format!("Edge Label: {}", self.edge_info.label),
            format!("Direction: {}", dir),
            format!("Path Mode: {}", self.path_mode),
        ];
        if (self.target_local_col_idx.is_some() || self.target_filter.is_some())
            && self.path_mode == PathMode::AnyShortest
        {
            params.push("Search: Bidirectional".to_string());
        }
        if self.min_hops != 1 || self.max_hops != 1 {
            if self.max_hops == u64::MAX {
                params.push(format!("Hops: {{{},}}", self.min_hops));
            } else {
                params.push(format!("Hops: {{{},{}}}", self.min_hops, self.max_hops));
            }
        }
        params
    }

    fn estimated_cardinality(&self) -> usize {
        estimate_expand_cardinality(
            self.child.estimated_cardinality(),
            self.direction,
            self.min_hops,
            self.max_hops,
        )
    }

    fn children_count(&self) -> usize {
        1
    }

    fn child(&self, idx: usize) -> Option<&dyn PhysicalOperator> {
        if idx == 0 {
            Some(self.child.as_ref())
        } else {
            None
        }
    }

    fn child_arc(&self, idx: usize) -> Option<Arc<dyn PhysicalOperator>> {
        if idx == 0 {
            Some(self.child.clone())
        } else {
            None
        }
    }

    fn get_operator_state(&self, ctx: &ExecutionContext) -> Result<Box<dyn OperatorState>> {
        self.externalized.store(false, Ordering::Release);
        self.peak_memory_bytes.store(0, Ordering::Release);
        // Eagerly acquire the graph projection index handle during state init
        // to avoid RwLock contention on every execute() call.
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

        Ok(Box::new(GraphShortestPathState {
            cached_snapshot: Some(snapshot),
            forward_neighbor_scratch: Vec::new(),
            backward_neighbor_scratch: Vec::new(),
            valid_targets: None,
            valid_targets_computed: false,
            output_buffer: GraphPathOutputBuffer::new(self.emit_path_info),
            resume_state: None,
            input_row_cursor: 0,
            lane_seen_scratch: Vec::new(),
            lane_visit_scratch: Vec::new(),
            lane_visit_next_scratch: Vec::new(),
            temporary_memory_state: None,
        }))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn runtime_memory_stats(&self) -> ExplainRuntimeStats {
        ExplainRuntimeStats {
            spilled: Some(self.externalized.load(Ordering::Acquire)),
            peak_memory_bytes: Some(self.peak_memory_bytes.load(Ordering::Acquire) as u64),
            temp_storage_bytes: None,
        }
    }

    fn execute(
        &self,
        ctx: &ExecutionContext,
        input: &Chunk,
        chunk: &mut Chunk,
        _gstate: &dyn GlobalOperatorState,
        state: &mut dyn OperatorState,
    ) -> Result<OperatorResultType> {
        if input.is_empty() {
            let op_state = state
                .as_any_mut()
                .downcast_mut::<GraphShortestPathState>()
                .expect("Invalid state type for GraphShortestPath");
            op_state.resume_state = None;
            op_state.input_row_cursor = 0;
            op_state.output_buffer.clear();
            if let Some(temp_state) = &op_state.temporary_memory_state {
                temp_state.set_zero();
            }
            return Ok(OperatorResultType::NeedMoreInput);
        }

        let op_state = state
            .as_any_mut()
            .downcast_mut::<GraphShortestPathState>()
            .expect("Invalid state type for GraphShortestPath");

        // Use cached index handle (acquired in get_operator_state)
        let snapshot = op_state
            .cached_snapshot
            .as_ref()
            .ok_or_else(|| {
                paro_error::internal(format!(
                    "Graph read snapshot for \"{}\" not cached in state",
                    self.graph_name
                ))
            })?
            .clone();
        let snapshot = &snapshot;
        let index = snapshot.base().as_ref();
        if self.target_local_col_idx.is_none()
            && self.target_filter.is_some()
            && !op_state.valid_targets_computed
        {
            let bitset = self.evaluate_target_filter(ctx, index)?;
            op_state.valid_targets = Some(bitset);
            op_state.valid_targets_computed = true;
        }
        let valid_targets = op_state.valid_targets.as_deref();

        let edge_label = &self.edge_info.label;
        let vmap = index.vertex_map(&self.target_label).ok_or_else(|| {
            paro_error::internal(format!(
                "Vertex map for \"{}\" not found",
                self.target_label
            ))
        })?;

        let num_vertices = vmap.num_vertices();
        let ncols = input.column_count();

        // src_local_id is at column ncols-2 (second to last)
        let src_col = input
            .column(self.source_local_col_idx)
            .ok_or_else(|| paro_error::internal("Missing source local_id column"))?;
        let target_col = self
            .target_local_col_idx
            .and_then(|target_local_col_idx| input.column(target_local_col_idx));
        let singleton_target = valid_targets.and_then(single_local_from_bitset);
        let bound_target_mode = self.path_mode == PathMode::AnyShortest
            && (target_col.is_some() || singleton_target.is_some());
        let single_source_mode = !bound_target_mode && self.emit_path_info;

        op_state.output_buffer.clear();

        while !op_state.output_buffer.is_full() {
            if let Some(resume_state) = op_state.resume_state.take() {
                match resume_state {
                    GraphShortestPathResumeState::Lane(lane_state) => {
                        let temp_state = self.ensure_temporary_memory_state(ctx, op_state);
                        match self.advance_lane_state(
                            lane_state,
                            snapshot,
                            edge_label,
                            vmap,
                            &mut op_state.output_buffer,
                            &mut op_state.forward_neighbor_scratch,
                            &mut op_state.backward_neighbor_scratch,
                            Some(&temp_state),
                        ) {
                            LaneAdvanceResult::Suspended(next_state) => {
                                op_state.resume_state =
                                    Some(GraphShortestPathResumeState::Lane(next_state));
                                break;
                            }
                            LaneAdvanceResult::Finished {
                                seen,
                                visit,
                                visit_next,
                            } => {
                                op_state.lane_seen_scratch = seen;
                                op_state.lane_visit_scratch = visit;
                                op_state.lane_visit_next_scratch = visit_next;
                                temp_state.set_zero();
                            }
                        }
                    }
                    GraphShortestPathResumeState::SingleSource(single_source_state) => {
                        let temp_state = self.ensure_temporary_memory_state(ctx, op_state);
                        if let Some(next_state) = self.advance_single_source_state(
                            input,
                            single_source_state,
                            snapshot,
                            edge_label,
                            vmap,
                            &mut op_state.output_buffer,
                            &mut op_state.forward_neighbor_scratch,
                            &mut op_state.backward_neighbor_scratch,
                            Some(&temp_state),
                        )? {
                            op_state.resume_state =
                                Some(GraphShortestPathResumeState::SingleSource(next_state));
                            break;
                        } else {
                            temp_state.set_zero();
                        }
                    }
                }
                continue;
            }

            if op_state.input_row_cursor >= input.size() {
                break;
            }

            if bound_target_mode {
                let temp_state = self.ensure_temporary_memory_state(ctx, op_state);
                let frontier_threshold = self.graph_frontier_threshold(ctx, true)?;
                let row = op_state.input_row_cursor;
                op_state.input_row_cursor += 1;
                let src_local = src_col.get_u64(row).unwrap_or(0) as u32;
                let dst_local = target_col
                    .and_then(|col| col.get_u64(row))
                    .map(|value| value as u32)
                    .or(singleton_target)
                    .unwrap_or(0);
                self.bfs_shortest_between_bound_vertices(
                    input,
                    row,
                    src_local,
                    dst_local,
                    snapshot,
                    edge_label,
                    vmap,
                    &mut op_state.output_buffer,
                    &mut op_state.forward_neighbor_scratch,
                    &mut op_state.backward_neighbor_scratch,
                    ctx.buffer_pool().clone(),
                    frontier_threshold,
                    Some(&temp_state),
                )?;
                continue;
            }

            if single_source_mode {
                let temp_state = self.ensure_temporary_memory_state(ctx, op_state);
                let frontier_threshold = self.graph_frontier_threshold(ctx, true)?;
                let row = op_state.input_row_cursor;
                op_state.input_row_cursor += 1;
                let src_local = src_col.get_u64(row).unwrap_or(0) as u32;
                if let Some(state) = self.init_single_source_state(
                    input,
                    row,
                    src_local,
                    vmap,
                    ctx.buffer_pool().clone(),
                    frontier_threshold,
                )? {
                    if let Some(next_state) = self.advance_single_source_state(
                        input,
                        state,
                        snapshot,
                        edge_label,
                        vmap,
                        &mut op_state.output_buffer,
                        &mut op_state.forward_neighbor_scratch,
                        &mut op_state.backward_neighbor_scratch,
                        Some(&temp_state),
                    )? {
                        op_state.resume_state =
                            Some(GraphShortestPathResumeState::SingleSource(next_state));
                        break;
                    } else {
                        temp_state.set_zero();
                    }
                } else {
                    temp_state.set_zero();
                }
                continue;
            }

            let temp_state = self.ensure_temporary_memory_state(ctx, op_state);
            let batch_start = op_state.input_row_cursor;
            let batch_end = (batch_start + LANE_LIMIT).min(input.size());
            op_state.input_row_cursor = batch_end;
            let mut sources: Vec<(Vec<u64>, u32)> = Vec::with_capacity(batch_end - batch_start);
            for row in batch_start..batch_end {
                let src_local = src_col.get_u64(row).unwrap_or(0) as u32;
                sources.push((Self::collect_input_row_values(input, row), src_local));
            }
            if let Some(state) = self.init_lane_state(
                sources,
                num_vertices,
                &mut op_state.lane_seen_scratch,
                &mut op_state.lane_visit_scratch,
                &mut op_state.lane_visit_next_scratch,
            ) {
                match self.advance_lane_state(
                    state,
                    snapshot,
                    edge_label,
                    vmap,
                    &mut op_state.output_buffer,
                    &mut op_state.forward_neighbor_scratch,
                    &mut op_state.backward_neighbor_scratch,
                    Some(&temp_state),
                ) {
                    LaneAdvanceResult::Suspended(next_state) => {
                        op_state.resume_state =
                            Some(GraphShortestPathResumeState::Lane(next_state));
                        break;
                    }
                    LaneAdvanceResult::Finished {
                        seen,
                        visit,
                        visit_next,
                    } => {
                        op_state.lane_seen_scratch = seen;
                        op_state.lane_visit_scratch = visit;
                        op_state.lane_visit_next_scratch = visit_next;
                        temp_state.set_zero();
                    }
                }
            } else {
                temp_state.set_zero();
            }
        }

        if op_state.output_buffer.is_empty() {
            *chunk = Chunk::init_empty(&self.output_types);
            if op_state.resume_state.is_none() && op_state.input_row_cursor >= input.size() {
                op_state.input_row_cursor = 0;
                if let Some(temp_state) = &op_state.temporary_memory_state {
                    temp_state.set_zero();
                }
                return Ok(OperatorResultType::NeedMoreInput);
            }
            return Ok(OperatorResultType::HaveMoreOutput);
        }

        let has_more_output =
            op_state.resume_state.is_some() || op_state.input_row_cursor < input.size();
        let (output_rows, path_rows) = op_state.output_buffer.take();
        let count = output_rows.len();
        self.materialize_output_chunk(chunk, ncols, &output_rows, &path_rows);
        storage_metrics().add_graph_expand_rows(count);

        if has_more_output {
            Ok(OperatorResultType::HaveMoreOutput)
        } else {
            op_state.input_row_cursor = 0;
            if let Some(temp_state) = &op_state.temporary_memory_state {
                temp_state.set_zero();
            }
            Ok(OperatorResultType::NeedMoreInput)
        }
    }
}
