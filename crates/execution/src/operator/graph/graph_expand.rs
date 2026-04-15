//! Graph expand operator built on CSR adjacency.
//!
//! The operator appends `[edge_rowid, dst_local_id, dst_rowid]` for each hop.
//! Multi-hop expansion uses BFS and can pre-compute a target filter bitmap to
//! skip neighbors that would be rejected later.

use std::any::Any;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

use paro_catalog::entry::{CatalogEntryEnum, EdgeTableInfo};
use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::identity::GraphId;
use paro_common::types::LogicalType;
use paro_common::vector::Vector;
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
use super::graph_path::{
    collect_prefix_path, materialize_path_vectors, path_element_list_type, MaterializedPath,
    PathElementRef, PathEmitSpec, PATH_EDGES_OFFSET, PATH_LENGTH_OFFSET, PATH_VERTICES_OFFSET,
};
use super::spillable_frontier::{SpillableFrontier, SpillableFrontierCursor};
use super::spillable_parent_arrays::{ParentLookupState, SpillableParentArrays};

/// Default output batch size for expand operator.
const EXPAND_BATCH_SIZE: usize = 2048;
const GRAPH_FRONTIER_SPILL_THRESHOLD_ROWS: usize = EXPAND_BATCH_SIZE * 4;

#[derive(Debug)]
pub struct PhysicalGraphExpand {
    pub graph_name: String,
    pub edge_info: EdgeTableInfo,
    pub direction: ExpandDirection,
    pub source_label: String,
    pub target_label: String,
    pub source_local_col_idx: usize,
    /// Bound target local id column for ExpandInto fast path.
    pub target_local_col_idx: Option<usize>,
    pub min_hops: u64,
    pub max_hops: u64,
    /// Whether to emit path metadata columns after the standard output.
    pub emit_path_info: bool,
    /// Prefix path layout used to reconstruct vertices(p) / edges(p).
    pub path_emit_spec: PathEmitSpec,
    /// Optional filter on edge properties.
    /// Currently stored but not consumed during expansion — edge_filter
    /// requires late materialization and is still applied in GraphProject.
    /// Reserved for future optimization.
    pub edge_filter: Option<paro_planner::expression::Expression>,
    /// Optional filter on target vertex properties.
    /// Pre-computes a valid_targets BitSet on first execute, used to skip
    /// invalid neighbors during expansion.
    pub target_filter: Option<paro_planner::expression::Expression>,
    /// Target vertex table name for catalog lookup when target_filter is present.
    pub target_table_name: String,
    /// Schema name for catalog lookup when target_filter is present.
    pub schema_name: String,
    child: Arc<dyn PhysicalOperator>,
    output_types: Vec<LogicalType>,
    externalized: AtomicBool,
    peak_memory_bytes: AtomicUsize,
}

/// Multi-hop BFS state preserved across `HaveMoreOutput` calls.
///
/// Uses FixedBitSet (Vec<u64>) for visited set instead of HashSet<u32>,
/// and Vec<u32> dense frontier for cache-friendly traversal.
/// Supports output batching via hop-level cursors to avoid unbounded buffering.
///
/// When emit_path_info is true, maintains per-hop parent arrays
/// for O(hops) path reconstruction.
#[derive(Debug)]
struct MultiHopState {
    /// Current hop number being expanded (1-based).
    current_hop: u64,
    /// Frontier vertices for the current hop.
    frontier: SpillableFrontier,
    frontier_cursor: SpillableFrontierCursor,
    /// FixedBitSet for visited vertices (replaces HashSet<u32>).
    /// Bit at position `local_id` is set if the vertex has been visited.
    /// Memory: num_vertices / 8 bytes. O(1) test/set, no hash overhead.
    visited: Vec<u64>,
    /// Whether the current hop has been initialized.
    hop_initialized: bool,
    /// Cursor into frontier vertices for the current hop.
    hop_frontier_idx: usize,
    /// Cursor into CSR list for the current frontier vertex.
    hop_csr_idx: usize,
    /// Cursor into neighbor list for the current CSR.
    hop_neighbor_idx: usize,
    /// Per-hop dedup bitset to avoid duplicates within the hop.
    hop_seen: Vec<u64>,
    /// Next-hop frontier computed for the current hop (accumulated incrementally).
    next_frontier: SpillableFrontier,
    /// Per-hop parent tracking for path reconstruction, present only when
    /// `emit_path_info=true`.
    parents: Option<SpillableParentArrays>,
    parent_lookup_state: Option<ParentLookupState>,
}

/// Thread-local state for GraphExpand operator.
///
/// Caches the graph projection index handle and CSR/VertexIdMap references
/// to avoid repeated RwLock acquisitions on the hot path.
/// Supports output batching via input/neighbor cursors and HaveMoreOutput.
///
/// ## Target filter BitSet
/// `valid_targets` is a pre-computed BitSet (Vec<u64>) of target vertices
/// that pass the target_filter predicate. Each bit at position `local_id`
/// indicates whether that vertex is valid. During expansion, neighbors are
/// checked against this BitSet and skipped if invalid.
/// Memory: num_vertices / 8 bytes (10M vertices ≈ 1.2 MB).
#[derive(Debug)]
struct GraphExpandOperatorState {
    /// Cached graph projection index handle (acquired once in open phase).
    cached_snapshot: Option<GraphReadSnapshot>,
    /// Scratch space for forward delta-aware neighbor merges.
    forward_neighbor_scratch: Vec<(u32, u64)>,
    /// Scratch space for backward delta-aware neighbor merges.
    backward_neighbor_scratch: Vec<(u32, u64)>,

    /// Pre-computed BitSet of valid target vertices.
    /// Bit at position `local_id` is set if the vertex passes target_filter.
    /// None means no target_filter or not yet computed.
    valid_targets: Option<Vec<u64>>,
    /// Whether valid_targets has been computed (to distinguish None = no filter
    /// from None = not yet computed).
    valid_targets_computed: bool,

    // --- Output backpressure control (single-hop) ---
    /// Current row in the input being processed.
    input_row_cursor: usize,
    /// Current neighbor index within the current input row's adjacency list.
    neighbor_cursor: usize,

    // --- Multi-hop state ---
    /// Multi-hop BFS state, present when processing a multi-hop expansion
    /// that spans multiple execute() calls.
    hop_state: Option<MultiHopState>,
    /// The input row values for the current multi-hop expansion.
    multi_hop_input_vals: Option<Vec<u64>>,
    /// Source local_id for the current multi-hop expansion.
    multi_hop_src: u32,
    /// Current input row index for multi-hop processing across execute() calls.
    multi_hop_row_cursor: usize,
    /// Whether multi-hop processing is in progress (has pending state).
    multi_hop_active: bool,
    temporary_memory_state: Option<Arc<TemporaryMemoryState>>,
}

impl OperatorState for GraphExpandOperatorState {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

impl PhysicalGraphExpand {
    fn multi_hop_workset_bytes(state: &MultiHopState) -> usize {
        let frontier_bytes =
            state.frontier.resident_memory_bytes() + state.next_frontier.resident_memory_bytes();
        let parent_bytes = state
            .parents
            .as_ref()
            .map(|parents| parents.current_in_memory_bytes())
            .unwrap_or(0);
        frontier_bytes + parent_bytes
    }

    fn update_multi_hop_temporary_memory(
        &self,
        ctx: &ExecutionContext,
        op_state: &mut GraphExpandOperatorState,
    ) -> Result<()> {
        let Some(hop_state) = op_state.hop_state.as_mut() else {
            if let Some(temp_state) = &op_state.temporary_memory_state {
                temp_state.set_zero();
            }
            return Ok(());
        };

        if op_state.temporary_memory_state.is_none() {
            let temp_state = ctx.temporary_memory_manager().register();
            temp_state.set_zero();
            op_state.temporary_memory_state = Some(temp_state);
        }
        let temp_state = op_state
            .temporary_memory_state
            .as_ref()
            .expect("graph expand temporary memory state initialized");

        let mut bytes = Self::multi_hop_workset_bytes(hop_state);
        if bytes == 0 {
            temp_state.set_zero();
            return Ok(());
        }

        temp_state.set_remaining_size_and_update_reservation(bytes);
        self.record_runtime_memory(temp_state, hop_state);
        if temp_state.get_reservation() < bytes {
            hop_state.frontier.ensure_external()?;
            hop_state.next_frontier.ensure_external()?;
            bytes = Self::multi_hop_workset_bytes(hop_state);
            if bytes == 0 {
                temp_state.set_zero();
            } else {
                temp_state.set_remaining_size_and_update_reservation(bytes);
                self.record_runtime_memory(temp_state, hop_state);
            }
        }
        Ok(())
    }

    fn graph_frontier_threshold(&self, ctx: &ExecutionContext) -> Result<usize> {
        let tmm_cfg = ctx.temporary_memory_manager().current_config();
        if tmm_cfg.force_external && !tmm_cfg.has_temporary_directory {
            return Err(paro_error::out_of_memory(
                "force_external requires a temporary directory (SET temp_directory)",
            ));
        }
        Ok(if tmm_cfg.force_external {
            0
        } else {
            GRAPH_FRONTIER_SPILL_THRESHOLD_ROWS
        })
    }

    fn record_runtime_memory(
        &self,
        temp_state: &Arc<TemporaryMemoryState>,
        hop_state: &MultiHopState,
    ) {
        self.peak_memory_bytes
            .fetch_max(temp_state.get_peak_reservation(), Ordering::AcqRel);
        if hop_state.frontier.is_external() || hop_state.next_frontier.is_external() {
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
        min_hops: u64,
        max_hops: u64,
        child: Arc<dyn PhysicalOperator>,
    ) -> Self {
        Self::with_filters(
            graph_name,
            edge_info,
            direction,
            source_label,
            target_label,
            source_local_col_idx,
            target_local_col_idx,
            min_hops,
            max_hops,
            false,
            PathEmitSpec::default(),
            None,
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
        min_hops: u64,
        max_hops: u64,
        emit_path_info: bool,
        path_emit_spec: PathEmitSpec,
        child: Arc<dyn PhysicalOperator>,
    ) -> Self {
        Self::with_filters(
            graph_name,
            edge_info,
            direction,
            source_label,
            target_label,
            source_local_col_idx,
            target_local_col_idx,
            min_hops,
            max_hops,
            emit_path_info,
            path_emit_spec,
            None,
            None,
            String::new(),
            String::new(),
            child,
        )
    }

    /// Pass Constructor that accepts edge_filter, target_filter,
    /// target_table_name, and schema_name.
    pub fn with_filters(
        graph_name: String,
        edge_info: EdgeTableInfo,
        direction: ExpandDirection,
        source_label: String,
        target_label: String,
        source_local_col_idx: usize,
        target_local_col_idx: Option<usize>,
        min_hops: u64,
        max_hops: u64,
        emit_path_info: bool,
        path_emit_spec: PathEmitSpec,
        edge_filter: Option<paro_planner::expression::Expression>,
        target_filter: Option<paro_planner::expression::Expression>,
        target_table_name: String,
        schema_name: String,
        child: Arc<dyn PhysicalOperator>,
    ) -> Self {
        let mut output_types = child.types().to_vec();
        // Standard 3 columns: edge_rowid, dst_local_id, dst_rowid
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
            min_hops,
            max_hops,
            emit_path_info,
            path_emit_spec,
            edge_filter,
            target_filter,
            target_table_name,
            schema_name,
            child,
            output_types,
            externalized: AtomicBool::new(false),
            peak_memory_bytes: AtomicUsize::new(0),
        }
    }

    fn materialize_single_hop_path(
        &self,
        input: &Chunk,
        row: usize,
        edge_rowid: u64,
        dst_rowid: u64,
    ) -> MaterializedPath {
        let mut path = collect_prefix_path(input, row, &self.path_emit_spec);
        path.edges.push(PathElementRef {
            table_oid: self.path_emit_spec.segment_edge_table_oid,
            rowid: edge_rowid,
        });
        path.vertices.push(PathElementRef {
            table_oid: self.path_emit_spec.segment_vertex_table_oid,
            rowid: dst_rowid,
        });
        path.length += 1;
        path
    }

    fn materialize_multi_hop_path(
        &self,
        input: &Chunk,
        row: usize,
        dst: u32,
        path_len: u64,
        vmap: &VertexIdMap,
        parents: &SpillableParentArrays,
        lookup_state: &mut ParentLookupState,
    ) -> Result<MaterializedPath> {
        let mut path = collect_prefix_path(input, row, &self.path_emit_spec);
        let mut segment_vertices = vec![dst];
        let mut segment_edges = Vec::with_capacity(path_len as usize);
        let mut current = dst;

        for hop_idx in (0..path_len as usize).rev() {
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
        path.length += path_len as i64;
        Ok(path)
    }

    fn expand_into_view_degree(
        &self,
        snapshot: &GraphReadSnapshot,
        edge_label: &str,
        local_id: u32,
        from_target_side: bool,
        forward_neighbor_scratch: &mut Vec<(u32, u64)>,
        backward_neighbor_scratch: &mut Vec<(u32, u64)>,
    ) -> usize {
        match (from_target_side, self.direction) {
            (false, ExpandDirection::Forward) => snapshot
                .neighbors_forward(edge_label, local_id, forward_neighbor_scratch)
                .map(|view| view.len())
                .unwrap_or(0),
            (false, ExpandDirection::Backward) => snapshot
                .neighbors_backward(edge_label, local_id, backward_neighbor_scratch)
                .map(|view| view.len())
                .unwrap_or(0),
            (false, ExpandDirection::Both) => {
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
            (true, ExpandDirection::Forward) => snapshot
                .neighbors_backward(edge_label, local_id, backward_neighbor_scratch)
                .map(|view| view.len())
                .unwrap_or(0),
            (true, ExpandDirection::Backward) => snapshot
                .neighbors_forward(edge_label, local_id, forward_neighbor_scratch)
                .map(|view| view.len())
                .unwrap_or(0),
            (true, ExpandDirection::Both) => {
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
        }
    }

    fn for_each_expand_into_neighbor<F>(
        &self,
        snapshot: &GraphReadSnapshot,
        edge_label: &str,
        local_id: u32,
        from_target_side: bool,
        forward_neighbor_scratch: &mut Vec<(u32, u64)>,
        backward_neighbor_scratch: &mut Vec<(u32, u64)>,
        mut callback: F,
    ) where
        F: FnMut(u32, u64),
    {
        match (from_target_side, self.direction) {
            (false, ExpandDirection::Forward) => {
                if let Some(view) =
                    snapshot.neighbors_forward(edge_label, local_id, forward_neighbor_scratch)
                {
                    for idx in 0..view.len() {
                        if let Some((neighbor, edge_rowid)) = view.pair_at(idx) {
                            callback(neighbor, edge_rowid);
                        }
                    }
                }
            }
            (false, ExpandDirection::Backward) => {
                if let Some(view) =
                    snapshot.neighbors_backward(edge_label, local_id, backward_neighbor_scratch)
                {
                    for idx in 0..view.len() {
                        if let Some((neighbor, edge_rowid)) = view.pair_at(idx) {
                            callback(neighbor, edge_rowid);
                        }
                    }
                }
            }
            (false, ExpandDirection::Both) => {
                if let Some(view) =
                    snapshot.neighbors_forward(edge_label, local_id, forward_neighbor_scratch)
                {
                    for idx in 0..view.len() {
                        if let Some((neighbor, edge_rowid)) = view.pair_at(idx) {
                            callback(neighbor, edge_rowid);
                        }
                    }
                }
                if let Some(view) =
                    snapshot.neighbors_backward(edge_label, local_id, backward_neighbor_scratch)
                {
                    for idx in 0..view.len() {
                        if let Some((neighbor, edge_rowid)) = view.pair_at(idx) {
                            callback(neighbor, edge_rowid);
                        }
                    }
                }
            }
            (true, ExpandDirection::Forward) => {
                if let Some(view) =
                    snapshot.neighbors_backward(edge_label, local_id, backward_neighbor_scratch)
                {
                    for idx in 0..view.len() {
                        if let Some((neighbor, edge_rowid)) = view.pair_at(idx) {
                            callback(neighbor, edge_rowid);
                        }
                    }
                }
            }
            (true, ExpandDirection::Backward) => {
                if let Some(view) =
                    snapshot.neighbors_forward(edge_label, local_id, forward_neighbor_scratch)
                {
                    for idx in 0..view.len() {
                        if let Some((neighbor, edge_rowid)) = view.pair_at(idx) {
                            callback(neighbor, edge_rowid);
                        }
                    }
                }
            }
            (true, ExpandDirection::Both) => {
                if let Some(view) =
                    snapshot.neighbors_backward(edge_label, local_id, backward_neighbor_scratch)
                {
                    for idx in 0..view.len() {
                        if let Some((neighbor, edge_rowid)) = view.pair_at(idx) {
                            callback(neighbor, edge_rowid);
                        }
                    }
                }
                if let Some(view) =
                    snapshot.neighbors_forward(edge_label, local_id, forward_neighbor_scratch)
                {
                    for idx in 0..view.len() {
                        if let Some((neighbor, edge_rowid)) = view.pair_at(idx) {
                            callback(neighbor, edge_rowid);
                        }
                    }
                }
            }
        }
    }

    /// Multi-hop BFS expansion using FixedBitSet for visited set
    /// and Vec<u32> dense frontier. Supports HaveMoreOutput via MultiHopState.
    ///
    /// When emit_path_info is true, maintains per-hop parent arrays
    /// for O(hops) path reconstruction.
    ///
    /// Returns a batch of (edge_rowid, dst_local_id, path_length) rows.
    fn expand_multi_hop_vectorized(
        &self,
        _input_vals: &[u64],
        _src: u32,
        snapshot: &GraphReadSnapshot,
        edge_label: &str,
        vmap: &VertexIdMap,
        valid_targets: Option<&[u64]>,
        state: &mut MultiHopState,
        forward_neighbor_scratch: &mut Vec<(u32, u64)>,
        backward_neighbor_scratch: &mut Vec<(u32, u64)>,
        batch_limit: usize,
    ) -> Result<Vec<(u64, u32, u64)>> {
        let mut results: Vec<(u64, u32, u64)> = Vec::new();
        if batch_limit == 0 {
            return Ok(results);
        }

        let num_vertices = vmap.num_vertices() as usize;
        let num_words = (num_vertices + 63) / 64;
        if state.hop_seen.len() != num_words {
            state.hop_seen = vec![0u64; num_words];
        }

        while state.current_hop <= self.max_hops {
            if state.frontier.is_empty() {
                break;
            }
            storage_metrics().set_graph_frontier_size(state.frontier.len());

            // Initialize per-hop state if starting a new hop.
            if !state.hop_initialized {
                state.hop_seen.fill(0);
                state.next_frontier.clear()?;
                state.hop_frontier_idx = 0;
                state.hop_csr_idx = 0;
                state.hop_neighbor_idx = 0;
                state.hop_initialized = true;
            }

            let hop = state.current_hop;

            while state.hop_frontier_idx < state.frontier.len() {
                let cur = state
                    .frontier
                    .get(state.hop_frontier_idx, &mut state.frontier_cursor)?;
                while state.hop_csr_idx < 2 {
                    let view = match state.hop_csr_idx {
                        0 if self.direction == ExpandDirection::Forward
                            || self.direction == ExpandDirection::Both =>
                        {
                            snapshot.neighbors_forward(edge_label, cur, forward_neighbor_scratch)
                        }
                        1 if self.direction == ExpandDirection::Backward
                            || self.direction == ExpandDirection::Both =>
                        {
                            snapshot.neighbors_backward(edge_label, cur, backward_neighbor_scratch)
                        }
                        _ => None,
                    };
                    let Some(view) = view else {
                        state.hop_neighbor_idx = 0;
                        state.hop_csr_idx += 1;
                        continue;
                    };
                    while state.hop_neighbor_idx < view.len() {
                        let i = state.hop_neighbor_idx;
                        state.hop_neighbor_idx += 1;
                        let Some((dst, edge_rowid)) = view.pair_at(i) else {
                            continue;
                        };

                        // Skip already visited vertices (previous hops).
                        if bitset_test(&state.visited, dst) {
                            continue;
                        }
                        // Deduplicate within this hop using a local BitSet.
                        if bitset_test(&state.hop_seen, dst) {
                            continue;
                        }
                        bitset_set(&mut state.hop_seen, dst);
                        state.next_frontier.push(dst)?;

                        // Record parent for path reconstruction.
                        if let Some(parents) = state.parents.as_mut() {
                            parents.set_parent(dst, cur, edge_rowid);
                        }

                        // Emit results for hops within [min_hops, max_hops].
                        if hop >= self.min_hops {
                            // Apply target_filter at output time only.
                            if let Some(bitset) = valid_targets {
                                if !bitset_test(bitset, dst) {
                                    continue;
                                }
                            }
                            results.push((edge_rowid, dst, hop));
                            if results.len() >= batch_limit {
                                return Ok(results);
                            }
                        }
                    }
                    state.hop_neighbor_idx = 0;
                    state.hop_csr_idx += 1;
                }
                state.hop_csr_idx = 0;
                state.hop_frontier_idx += 1;
            }

            // Commit next frontier and advance hop.
            let mut next_frontier_cursor = SpillableFrontierCursor::default();
            for idx in 0..state.next_frontier.len() {
                let dst = state.next_frontier.get(idx, &mut next_frontier_cursor)?;
                bitset_set(&mut state.visited, dst);
            }
            if let Some(parents) = state.parents.as_mut() {
                parents.commit_hop()?;
            }
            state.frontier = state.next_frontier.take();
            state.frontier_cursor = SpillableFrontierCursor::default();
            state.current_hop += 1;
            state.hop_initialized = false;
        }

        Ok(results)
    }
}

impl PhysicalOperator for PhysicalGraphExpand {
    fn operator_type(&self) -> PhysicalOperatorType {
        PhysicalOperatorType::GraphExpand
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
        ];
        if (self.target_local_col_idx.is_some() || self.target_filter.is_some())
            && self.min_hops == 1
            && self.max_hops == 1
        {
            params.push("Mode: ExpandInto".to_string());
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

        Ok(Box::new(GraphExpandOperatorState {
            cached_snapshot: Some(snapshot),
            forward_neighbor_scratch: Vec::new(),
            backward_neighbor_scratch: Vec::new(),
            valid_targets: None,
            valid_targets_computed: false,
            input_row_cursor: 0,
            neighbor_cursor: 0,
            hop_state: None,
            multi_hop_input_vals: None,
            multi_hop_src: 0,
            multi_hop_row_cursor: 0,
            multi_hop_active: false,
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
                .downcast_mut::<GraphExpandOperatorState>()
                .expect("Invalid state type for GraphExpand");
            if let Some(temp_state) = &op_state.temporary_memory_state {
                temp_state.set_zero();
            }
            return Ok(OperatorResultType::NeedMoreInput);
        }

        let op_state = state
            .as_any_mut()
            .downcast_mut::<GraphExpandOperatorState>()
            .expect("Invalid state type for GraphExpand");

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

        // Compute valid_targets BitSet on first execute if target_filter is present
        if self.target_local_col_idx.is_none()
            && self.target_filter.is_some()
            && !op_state.valid_targets_computed
        {
            let bitset = self.evaluate_target_filter(ctx, index)?;
            op_state.valid_targets = Some(bitset);
            op_state.valid_targets_computed = true;
        }
        let valid_targets = op_state.valid_targets.clone();
        let valid_targets = valid_targets.as_deref();

        let edge_label = &self.edge_info.label;
        let vmap = index.vertex_map(&self.target_label).ok_or_else(|| {
            paro_error::internal(format!(
                "Vertex map for \"{}\" not found",
                self.target_label
            ))
        })?;

        let ncols = input.column_count();
        let src_col = input
            .column(self.source_local_col_idx)
            .ok_or_else(|| paro_error::internal("Missing source local_id column"))?;

        // Vectorized output with Chunk buffer and HaveMoreOutput backpressure.
        // Pre-allocate output vectors for batch_size rows.
        let ocols = self.output_types.len();
        let mut vectors: Vec<Vector> = self
            .output_types
            .iter()
            .map(|t| {
                let mut v = Vector::with_capacity(t.clone(), EXPAND_BATCH_SIZE);
                v.set_len(EXPAND_BATCH_SIZE);
                v
            })
            .collect();
        let mut out_row = 0usize;
        let mut dst_locals: Vec<u32> = Vec::with_capacity(EXPAND_BATCH_SIZE);
        let mut path_rows: Vec<MaterializedPath> = Vec::with_capacity(EXPAND_BATCH_SIZE);

        // Resume from saved cursor position if we returned HaveMoreOutput previously.
        let start_row = op_state.input_row_cursor;
        let start_nbr = op_state.neighbor_cursor;

        // Reset cursors — they'll be set again if we need to suspend.
        op_state.input_row_cursor = 0;
        op_state.neighbor_cursor = 0;

        let mut have_more = false;

        if self.min_hops == 1 && self.max_hops == 1 {
            if let Some(temp_state) = &op_state.temporary_memory_state {
                temp_state.set_zero();
            }
            // ── Single-hop vectorized expansion ──
            'outer: for row in start_row..input.size() {
                let src_local = src_col.get_u64(row).unwrap_or(0) as u32;

                // Collect input row values once.
                let mut vals = Vec::with_capacity(ncols);
                for c in 0..ncols {
                    vals.push(input.column(c).and_then(|v| v.get_u64(row)).unwrap_or(0));
                }

                // Determine the starting neighbor index for this row.
                // If we're resuming from a previous HaveMoreOutput, start_nbr applies
                // only to the first row we process (start_row).
                let nbr_start = if row == start_row { start_nbr } else { 0 };
                let target_local = self
                    .target_local_col_idx
                    .and_then(|idx| input.column(idx))
                    .and_then(|col| col.get_u64(row))
                    .map(|value| value as u32)
                    .or_else(|| valid_targets.and_then(single_local_from_bitset));
                let mut global_nbr_idx = 0usize;

                if let Some(target_local) = target_local {
                    let source_degree = self.expand_into_view_degree(
                        snapshot,
                        edge_label,
                        src_local,
                        false,
                        &mut op_state.forward_neighbor_scratch,
                        &mut op_state.backward_neighbor_scratch,
                    );
                    let target_degree = self.expand_into_view_degree(
                        snapshot,
                        edge_label,
                        target_local,
                        true,
                        &mut op_state.forward_neighbor_scratch,
                        &mut op_state.backward_neighbor_scratch,
                    );
                    let search_from_source = source_degree <= target_degree;
                    let needle = if search_from_source {
                        target_local
                    } else {
                        src_local
                    };
                    self.for_each_expand_into_neighbor(
                        snapshot,
                        edge_label,
                        if search_from_source {
                            src_local
                        } else {
                            target_local
                        },
                        !search_from_source,
                        &mut op_state.forward_neighbor_scratch,
                        &mut op_state.backward_neighbor_scratch,
                        |neighbor, edge_rowid| {
                            if have_more {
                                return;
                            }
                            if global_nbr_idx < nbr_start {
                                global_nbr_idx += 1;
                                return;
                            }
                            if neighbor != needle {
                                global_nbr_idx += 1;
                                return;
                            }

                            for (ci, &val) in vals.iter().enumerate() {
                                if ci < ocols {
                                    match &self.output_types[ci] {
                                        LogicalType::BigInt => {
                                            vectors[ci].set_i64(out_row, val as i64)
                                        }
                                        _ => vectors[ci].set_u64(out_row, val),
                                    }
                                }
                            }
                            let base = ncols;
                            let dst_rowid = vmap.local_to_rowid(target_local);
                            if base < ocols {
                                vectors[base].set_u64(out_row, edge_rowid);
                            }
                            if base + 1 < ocols {
                                vectors[base + 1].set_u64(out_row, target_local as u64);
                            }
                            if base + 2 < ocols {
                                vectors[base + 2].set_u64(out_row, dst_rowid);
                            }
                            dst_locals.push(target_local);
                            if self.emit_path_info {
                                path_rows.push(self.materialize_single_hop_path(
                                    input, row, edge_rowid, dst_rowid,
                                ));
                            }

                            out_row += 1;
                            global_nbr_idx += 1;
                            if out_row >= EXPAND_BATCH_SIZE {
                                op_state.input_row_cursor = row;
                                op_state.neighbor_cursor = global_nbr_idx;
                                have_more = true;
                            }
                        },
                    );
                    if have_more {
                        break 'outer;
                    }
                } else {
                    // Flatten all neighbors across CSR directions into a single iteration.
                    // We need a global neighbor index to support suspend/resume.
                    let forward_view = if self.direction == ExpandDirection::Forward
                        || self.direction == ExpandDirection::Both
                    {
                        snapshot.neighbors_forward(
                            edge_label,
                            src_local,
                            &mut op_state.forward_neighbor_scratch,
                        )
                    } else {
                        None
                    };
                    let backward_view = if self.direction == ExpandDirection::Backward
                        || self.direction == ExpandDirection::Both
                    {
                        snapshot.neighbors_backward(
                            edge_label,
                            src_local,
                            &mut op_state.backward_neighbor_scratch,
                        )
                    } else {
                        None
                    };
                    for view in [forward_view, backward_view].into_iter().flatten() {
                        for i in 0..view.len() {
                            let Some((dst, edge_rowid)) = view.pair_at(i) else {
                                continue;
                            };
                            if global_nbr_idx < nbr_start {
                                global_nbr_idx += 1;
                                continue;
                            }
                            // Skip neighbors that don't pass target_filter
                            if let Some(bitset) = valid_targets {
                                if !bitset_test(bitset, dst) {
                                    global_nbr_idx += 1;
                                    continue;
                                }
                            }

                            for (ci, &val) in vals.iter().enumerate() {
                                if ci < ocols {
                                    match &self.output_types[ci] {
                                        LogicalType::BigInt => {
                                            vectors[ci].set_i64(out_row, val as i64)
                                        }
                                        _ => vectors[ci].set_u64(out_row, val),
                                    }
                                }
                            }
                            let base = ncols;
                            if base < ocols {
                                vectors[base].set_u64(out_row, edge_rowid);
                            }
                            if base + 1 < ocols {
                                vectors[base + 1].set_u64(out_row, dst as u64);
                            }
                            if base + 2 < ocols {
                                vectors[base + 2].set_u64(out_row, 0);
                            }
                            dst_locals.push(dst);
                            if self.emit_path_info {
                                let dst_rowid = vmap.local_to_rowid(dst);
                                path_rows.push(self.materialize_single_hop_path(
                                    input, row, edge_rowid, dst_rowid,
                                ));
                            }

                            out_row += 1;
                            global_nbr_idx += 1;
                            if out_row >= EXPAND_BATCH_SIZE {
                                op_state.input_row_cursor = row;
                                op_state.neighbor_cursor = global_nbr_idx;
                                have_more = true;
                                break 'outer;
                            }
                        }
                    }
                }
            }

            if !dst_locals.is_empty() && ncols + 2 < ocols {
                let dst_rowids = vmap.batch_local_to_rowid(&dst_locals);
                for (i, rowid) in dst_rowids.iter().enumerate() {
                    vectors[ncols + 2].set_u64(i, *rowid);
                }
            }
        } else {
            // ── Multi-hop vectorized expansion with HaveMoreOutput ──
            // Uses FixedBitSet for visited set and supports batch-limited output.

            // Check if we're resuming from a previous HaveMoreOutput.
            let resume_row = if op_state.multi_hop_active {
                op_state.multi_hop_row_cursor
            } else {
                start_row
            };

            'multi_outer: for row in resume_row..input.size() {
                let src_local = src_col.get_u64(row).unwrap_or(0) as u32;
                let mut vals = Vec::with_capacity(ncols);
                for c in 0..ncols {
                    vals.push(input.column(c).and_then(|v| v.get_u64(row)).unwrap_or(0));
                }

                // Initialize or resume MultiHopState for this input row.
                if op_state.hop_state.is_none() {
                    let num_vertices = vmap.num_vertices() as usize;
                    let num_words = (num_vertices + 63) / 64;
                    let mut visited_bitset = vec![0u64; num_words];
                    bitset_set(&mut visited_bitset, src_local);
                    let buffer_pool = ctx.buffer_pool().clone();
                    let frontier_threshold = self.graph_frontier_threshold(ctx)?;
                    let mut frontier =
                        SpillableFrontier::new(buffer_pool.clone(), frontier_threshold);
                    frontier.push(src_local)?;
                    let next_frontier =
                        SpillableFrontier::new(buffer_pool.clone(), frontier_threshold);

                    op_state.hop_state = Some(MultiHopState {
                        current_hop: 1,
                        frontier,
                        frontier_cursor: SpillableFrontierCursor::default(),
                        visited: visited_bitset,
                        hop_initialized: false,
                        hop_frontier_idx: 0,
                        hop_csr_idx: 0,
                        hop_neighbor_idx: 0,
                        hop_seen: vec![0u64; num_words],
                        next_frontier,
                        parents: self
                            .emit_path_info
                            .then(|| SpillableParentArrays::new(num_vertices, buffer_pool)),
                        parent_lookup_state: self.emit_path_info.then(ParentLookupState::new),
                    });
                    if let Some(hop_state) = op_state.hop_state.as_ref() {
                        if hop_state.frontier.is_external() || hop_state.next_frontier.is_external()
                        {
                            self.externalized.store(true, Ordering::Release);
                        }
                    }
                    op_state.multi_hop_input_vals = Some(vals.clone());
                    op_state.multi_hop_src = src_local;
                    self.update_multi_hop_temporary_memory(ctx, op_state)?;
                }

                let remaining = EXPAND_BATCH_SIZE - out_row;
                let results = {
                    let hop_state = op_state.hop_state.as_mut().unwrap();
                    self.expand_multi_hop_vectorized(
                        &vals,
                        src_local,
                        snapshot,
                        edge_label,
                        vmap,
                        valid_targets,
                        hop_state,
                        &mut op_state.forward_neighbor_scratch,
                        &mut op_state.backward_neighbor_scratch,
                        remaining,
                    )?
                };

                // Write results into output vectors.
                let row_start = out_row;
                let mut dst_locals: Vec<u32> = Vec::with_capacity(results.len());
                for &(eid, dst, path_len) in &results {
                    for (ci, &val) in vals.iter().enumerate() {
                        if ci < ocols {
                            match &self.output_types[ci] {
                                LogicalType::BigInt => vectors[ci].set_i64(out_row, val as i64),
                                _ => vectors[ci].set_u64(out_row, val),
                            }
                        }
                    }
                    let base = ncols;
                    if base < ocols {
                        vectors[base].set_u64(out_row, eid);
                    }
                    if base + 1 < ocols {
                        vectors[base + 1].set_u64(out_row, dst as u64);
                    }
                    if base + 2 < ocols {
                        vectors[base + 2].set_u64(out_row, 0);
                    }
                    dst_locals.push(dst);
                    if self.emit_path_info {
                        let hop_state = op_state.hop_state.as_mut().unwrap();
                        let parents = hop_state.parents.as_ref().ok_or_else(|| {
                            paro_error::internal(
                                "GraphExpand multi-hop path materialization missing parent state",
                            )
                        })?;
                        let lookup_state =
                            hop_state.parent_lookup_state.as_mut().ok_or_else(|| {
                                paro_error::internal(
                                "GraphExpand multi-hop path materialization missing lookup state",
                            )
                            })?;
                        path_rows.push(self.materialize_multi_hop_path(
                            input,
                            row,
                            dst,
                            path_len,
                            vmap,
                            parents,
                            lookup_state,
                        )?);
                    }
                    out_row += 1;
                }
                self.update_multi_hop_temporary_memory(ctx, op_state)?;
                if !dst_locals.is_empty() && ncols + 2 < ocols {
                    let dst_rowids = vmap.batch_local_to_rowid(&dst_locals);
                    for (i, rowid) in dst_rowids.iter().enumerate() {
                        vectors[ncols + 2].set_u64(row_start + i, *rowid);
                    }
                }

                // Check if the BFS for this row is complete.
                let bfs_done = {
                    let hs = op_state.hop_state.as_ref().unwrap();
                    hs.frontier.is_empty() || hs.current_hop > self.max_hops
                };

                if bfs_done {
                    // BFS complete for this input row, move to next.
                    op_state.hop_state = None;
                    op_state.multi_hop_input_vals = None;
                    self.update_multi_hop_temporary_memory(ctx, op_state)?;
                } else {
                    // BFS not complete — batch is full, save state and return.
                    op_state.multi_hop_row_cursor = row;
                    op_state.multi_hop_active = true;
                    have_more = true;
                    break 'multi_outer;
                }

                if out_row >= EXPAND_BATCH_SIZE {
                    // Batch full after completing this row's BFS.
                    // If there are more input rows, signal HaveMoreOutput.
                    if row + 1 < input.size() {
                        op_state.multi_hop_row_cursor = row + 1;
                        op_state.multi_hop_active = true;
                        have_more = true;
                    }
                    break 'multi_outer;
                }
            }

            // If we finished all rows, clear multi-hop active state.
            if !have_more {
                op_state.multi_hop_active = false;
                op_state.multi_hop_row_cursor = 0;
                self.update_multi_hop_temporary_memory(ctx, op_state)?;
            }
        }

        if out_row == 0 {
            *chunk = Chunk::init_empty(&self.output_types);
            return Ok(OperatorResultType::NeedMoreInput);
        }

        // Trim vectors to actual output size and build Chunk.
        let path_base = ncols + 3;
        let path_vectors = if self.emit_path_info {
            Some(materialize_path_vectors(&path_rows))
        } else {
            None
        };

        let mut arcs: Vec<Arc<Vector>> = Vec::with_capacity(ocols);
        for (idx, mut v) in vectors.into_iter().enumerate() {
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
            v.set_len(out_row);
            arcs.push(Arc::new(v));
        }
        *chunk = Chunk::from_arc_vectors(arcs);
        chunk.set_cardinality(out_row);
        storage_metrics().add_graph_expand_rows(out_row);

        if have_more {
            Ok(OperatorResultType::HaveMoreOutput)
        } else {
            Ok(OperatorResultType::NeedMoreInput)
        }
    }
}

impl PhysicalGraphExpand {
    /// Evaluate target_filter against the target vertex table and
    /// build a BitSet (Vec<u64>) marking valid target local IDs.
    ///
    /// Scans the target vertex table with column pruning, evaluates the
    /// predicate via ExpressionExecutor, and maps matching rowids to local_ids
    /// using VertexIdMap. The resulting BitSet has bit `local_id` set for each
    /// vertex that passes the filter.
    ///
    /// Memory: num_vertices / 8 bytes (10M vertices ≈ 1.2 MB).
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
        // Allocate BitSet: ceil(num_vertices / 64) words
        let num_words = (num_vertices + 63) / 64;
        let mut bitset = vec![0u64; num_words];

        // Resolve the target vertex table name.
        // For forward edges, the target is the destination vertex table.
        // For backward edges, the target is the source vertex table.
        let target_table = if self.direction == ExpandDirection::Backward {
            &self.edge_info.source_vertex_table
        } else {
            &self.edge_info.destination_vertex_table
        };

        // Use target_table_name if available (set from GraphExpand),
        // otherwise fall back to edge_info.
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
            _ => {
                return Err(paro_error::wrong_object_type("table", table_name));
            }
        };
        let storage = table.get_storage().ok_or_else(|| {
            paro_error::internal(format!(
                "Target vertex table \"{}\" has no storage",
                table_name
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

        // Remap filter column references to match pruned scan output positions.
        let remapped_filter = remap_filter_columns(filter, &filter_col_ids);
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
                let passes = bool_col.get_bool(row).unwrap_or(false);
                if passes {
                    let rowid = rowid_col.get_i64(row).unwrap_or(0) as u64;
                    if let Some(local_id) = vertex_map.rowid_to_local(rowid) {
                        bitset_set(&mut bitset, local_id);
                    }
                }
            }
        }

        Ok(bitset)
    }
}

// ── BitSet helpers ──────────────────────────────────────────────────────────

/// Test whether bit at position `local_id` is set in the BitSet.
#[inline]
fn bitset_test(bitset: &[u64], local_id: u32) -> bool {
    let word_idx = (local_id / 64) as usize;
    let bit_idx = local_id % 64;
    if word_idx < bitset.len() {
        (bitset[word_idx] >> bit_idx) & 1 != 0
    } else {
        false
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

/// Set bit at position `local_id` in the BitSet.
#[inline]
fn bitset_set(bitset: &mut [u64], local_id: u32) {
    let word_idx = (local_id / 64) as usize;
    let bit_idx = local_id % 64;
    if word_idx < bitset.len() {
        bitset[word_idx] |= 1u64 << bit_idx;
    }
}

// ── Filter expression helpers (shared with graph_scan.rs pattern) ───────────

/// Extract column IDs referenced by a filter expression.
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

/// Remap column references in a filter expression to match pruned scan output.
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
