// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Physical Hash Join Operator
//!
//!
//! ## Dependencies Check
//! - Allocator: ✅ Uses JoinHashTable with HashBuildStore
//! - BufferManager: ✅ Uses BufferPool through JoinHashTable
//!
//! ## Supported Join Types
//! - INNER JOIN
//! - LEFT OUTER JOIN
//! - RIGHT OUTER JOIN
//! - FULL OUTER JOIN
//! - LEFT SEMI JOIN
//! - LEFT ANTI JOIN
//! - MARK JOIN
//! - SINGLE JOIN
//! - RIGHT SEMI JOIN
//! - RIGHT ANTI JOIN
//!
//! finalized sink state internally. When acting as a source in the next
//! pipeline, `get_global_source_state` uses this stored state.
//!
//! ## Known Limitations
//! - No perfect hash join optimization yet
//! - No filter pushdown yet

use std::any::Any;
use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::memory::{
    MemoryAccountingClass, MemoryAccountingContext, MemoryDomain, MemoryOwner,
};
use paro_common::types::LogicalType;
use paro_common::vector::{SelectionVector, Vector};
use paro_planner::operator::join::{JoinCondition, JoinType};
use paro_storage::buffer::MemoryTag;
use paro_storage::row::{RadixPartitionedRowsBuilder, RowScanState, RowStore};

use crate::execution_context::ExecutionContext;
use crate::explain::types::ExplainRuntimeStats;
use crate::expression_executor::executor::ExpressionExecutor;
use crate::join_hashtable::join_hashtable::{JoinHashTable, JoinHashTableConfig};
use crate::join_hashtable::perfect_hash_join::PerfectHashJoinExecutor;
use crate::memory_runtime::{
    OperatorExternalMemoryTracker, ReclaimStats, Reclaimer, SharedRetainedObject, SpillCost,
};
use crate::operator::join::join_filter_pushdown::{
    JoinFilterGlobalState, JoinFilterLocalState, JoinFilterPushdownInfo, JoinFilterRuntimeStats,
};
use crate::operator::join::join_result_helpers::{
    construct_right_outer_scan_result, construct_semi_join_result,
};
use crate::operator::join::physical_comparison_join::PhysicalComparisonJoin;
use crate::operator::state::{
    GlobalOperatorState, GlobalSinkState, GlobalSourceState, LocalSinkState, LocalSourceState,
    OperatorSinkCombineInput, OperatorSinkFinalizeInput, OperatorSinkInput, OperatorSourceInput,
    OperatorState,
};
use crate::operator::PhysicalOperator;
use crate::operator_type::PhysicalOperatorType;
use crate::pipeline::build_state::PipelineBuildState;
use crate::pipeline::meta_pipeline::MetaPipeline;
use crate::pipeline::pipeline::Pipeline;
use crate::result_type::{
    OperatorFinalizeResultType, SinkCombineResultType, SinkFinalizeType, SinkResultType,
    SourceResultType,
};
use crate::spill::probe_spill::{ProbeSpill, ProbeSpillLocalState};

use super::payload_layout::BuildPayloadLayout;
use super::{external, payload_layout, probe_engine};

const HASH_JOIN_INITIAL_RADIX_BITS: usize = 4;
const HASH_JOIN_MAX_RADIX_BITS: usize = 12;
const HASH_JOIN_EXTERNAL_LOAD_FACTOR: f64 = 1.5;
const HASH_JOIN_SKEW_THRESHOLD: f64 = 0.8;
const HASH_JOIN_EXTERNAL_PARTITION_THREAD_CAP: usize = 4;
const HASH_JOIN_MEMORY_TAG: MemoryTag = MemoryTag::HashTable;
const HASH_JOIN_MEMORY_CLASS: MemoryAccountingClass = MemoryAccountingClass::Revocable;

/// Physical Hash Join operator.
///
/// Build side (right child) populates the hash table; probe side (left child) streams
/// against it. Right is build, left is probe.
///
pub struct HashJoin {
    /// Shared physical comparison join contract (`PhysicalComparisonJoin`).
    base: PhysicalComparisonJoin,
    /// Build payload layout after projection/residual-driven pruning.
    build_layout: BuildPayloadLayout,
    /// Stored global sink state (needed when this operator also acts as source).
    sink_state: Mutex<Option<Arc<dyn GlobalSinkState>>>,
    /// Filter pushdown information.
    filter_pushdown: Option<JoinFilterPushdownInfo>,
}

impl HashJoin {
    pub(super) fn base(&self) -> &PhysicalComparisonJoin {
        &self.base
    }

    pub(super) fn build_layout(&self) -> &BuildPayloadLayout {
        &self.build_layout
    }

    pub fn runtime_memory_stats(&self) -> ExplainRuntimeStats {
        let Some(sink_state) = self.sink_state() else {
            return ExplainRuntimeStats::default();
        };
        let Some(sink_state) = sink_state
            .as_any()
            .downcast_ref::<HashJoinGlobalSinkState>()
        else {
            return ExplainRuntimeStats::default();
        };
        ExplainRuntimeStats {
            spilled: Some(sink_state.externalized()),
            peak_memory_bytes: Some(sink_state.peak_reservation()),
            temp_storage_bytes: Some(sink_state.temp_storage_bytes()),
            ..Default::default()
        }
    }

    pub fn runtime_join_filter_stats(&self) -> Option<JoinFilterRuntimeStats> {
        let sink_state = self.sink_state()?;
        let sink_state = sink_state
            .as_any()
            .downcast_ref::<HashJoinGlobalSinkState>()?;
        sink_state
            .filter_gstate
            .as_ref()
            .and_then(JoinFilterGlobalState::runtime_stats)
    }

    pub fn join_filter_kind(&self) -> Option<&'static str> {
        self.filter_pushdown
            .as_ref()
            .map(JoinFilterPushdownInfo::filter_kind)
    }

    pub fn join_filter_target_condition_indices(&self) -> Option<Vec<usize>> {
        let filter_info = self.filter_pushdown.as_ref()?;
        let mut indices = filter_info
            .probe_info
            .iter()
            .map(|filter| filter.join_condition_idx)
            .collect::<Vec<_>>();
        indices.sort_unstable();
        indices.dedup();
        Some(indices)
    }

    pub(crate) fn join_type(&self) -> JoinType {
        self.base.join.join_type
    }

    pub(crate) fn conditions(&self) -> &[JoinCondition] {
        &self.base.conditions
    }

    pub(crate) fn equality_conditions(&self) -> &[JoinCondition] {
        &self.base.equality_conditions
    }

    pub(crate) fn build_pipelines_base(
        &self,
        op: &Arc<dyn PhysicalOperator>,
        current: &Arc<Pipeline>,
        meta_pipeline: &Arc<MetaPipeline>,
        state: &mut PipelineBuildState,
        build_rhs: bool,
    ) {
        self.base
            .join
            .build_join_pipelines(op, current, meta_pipeline, state, build_rhs);
    }

    fn validate_join_type(join_type: JoinType) -> Result<()> {
        if matches!(
            join_type,
            JoinType::Inner
                | JoinType::Left
                | JoinType::Right
                | JoinType::Outer
                | JoinType::Semi
                | JoinType::Anti
                | JoinType::Mark
                | JoinType::Single
                | JoinType::RightSemi
                | JoinType::RightAnti
        ) {
            Ok(())
        } else {
            Err(paro_error::not_implemented(format!(
                "{} hash join result construction",
                join_type
            )))
        }
    }

    pub fn new(
        left: Arc<dyn PhysicalOperator>,
        right: Arc<dyn PhysicalOperator>,
        join_type: JoinType,
        conditions: Vec<JoinCondition>,
        left_projection_map: Vec<usize>,
        right_projection_map: Vec<usize>,
    ) -> Result<Self> {
        let base = PhysicalComparisonJoin::new(
            left.clone(),
            right.clone(),
            join_type,
            conditions.clone(),
            left_projection_map,
            right_projection_map,
        );
        let build_layout = payload_layout::derive_build_payload_layout(
            join_type,
            right.types(),
            &base.join.right_projection_map,
            &base.residual_conditions,
        )?;

        Ok(Self {
            base,
            build_layout,
            sink_state: Mutex::new(None),
            filter_pushdown: None,
        })
    }

    /// Set filter pushdown information.
    pub fn set_filter_pushdown(&mut self, info: JoinFilterPushdownInfo) {
        self.filter_pushdown = Some(info);
    }

    /// Initialize a JoinHashTable for this operator.
    fn initialize_hash_table(
        &self,
        ctx: &ExecutionContext,
        retained_object: Arc<SharedRetainedObject>,
    ) -> Arc<JoinHashTable> {
        let owner: Arc<dyn MemoryOwner> = retained_object;
        let memory = MemoryAccountingContext::from_owner(
            owner,
            MemoryDomain::Host,
            HASH_JOIN_MEMORY_TAG,
            MemoryAccountingClass::Revocable,
        );
        Arc::new(JoinHashTable::new_with_memory(
            ctx.buffer_pool().clone(),
            ctx.allocator(paro_common::allocator::MemoryTag::BaseTable),
            self.base.equality_conditions.clone(),
            self.build_layout.build_payload_types.clone(),
            self.base.join.join_type,
            JoinHashTableConfig::default(),
            memory,
        ))
    }

    fn build_payload_chunk(&self, chunk: &Chunk) -> Result<Chunk> {
        if self.build_layout.build_payload_columns.is_empty() {
            let mut payload_chunk = Chunk::try_initialize(
                &self.build_layout.build_payload_types,
                chunk.size(),
                chunk.allocator().clone(),
            )?;
            payload_chunk.try_set_cardinality(chunk.size())?;
            return Ok(payload_chunk);
        }

        let is_identity_projection = self.build_layout.build_payload_columns.len()
            == chunk.column_count()
            && self
                .build_layout
                .build_payload_columns
                .iter()
                .copied()
                .enumerate()
                .all(|(output_idx, input_idx)| output_idx == input_idx);
        if is_identity_projection {
            return Ok(chunk.clone());
        }

        let payload_vectors = self
            .build_layout
            .build_payload_columns
            .iter()
            .map(|column_idx| Arc::clone(&chunk.data[*column_idx]))
            .collect::<Vec<_>>();
        let mut payload_chunk = Chunk::from_arc_vectors(payload_vectors, chunk.allocator().clone());
        payload_chunk.try_set_cardinality(chunk.size())?;
        Ok(payload_chunk)
    }

    fn materialize_key_vector(
        ctx: &ExecutionContext,
        vector: Arc<Vector>,
        logical_type: LogicalType,
        count: usize,
    ) -> Result<Arc<Vector>> {
        let allocator = ctx.allocator(paro_common::allocator::MemoryTag::BaseTable);
        let mut flat = Vector::try_new(logical_type, count.max(1), allocator)?;
        flat.try_copy_range(0, vector.as_ref(), 0, count)?;
        Ok(Arc::new(flat))
    }

    fn finalize_in_memory_hash_table(&self, gstate: &HashJoinGlobalSinkState) -> Result<()> {
        if gstate.finalized.load(Ordering::Acquire) {
            return Ok(());
        }

        let local_tables = {
            let mut tables = gstate.local_hash_tables.lock().unwrap();
            std::mem::take(&mut *tables)
        };

        for local_ht in local_tables {
            gstate.hash_table.merge(local_ht)?;
        }

        gstate.hash_table.finalize()?;

        if gstate.hash_table.count() > 0 && self.base.join.join_type == JoinType::Inner {
            let mut perfect_executor = PerfectHashJoinExecutor::new();
            if let Ok((Some(min), Some(max))) =
                perfect_executor.gather_statistics(&gstate.hash_table)
            {
                if perfect_executor.can_do_perfect_hash_join(
                    self.base.join.join_type,
                    &gstate.hash_table.conditions,
                    &gstate.hash_table,
                    min,
                    max,
                ) {
                    if perfect_executor
                        .build_perfect_hash_table(&gstate.hash_table)
                        .is_ok()
                    {
                        let mut lock = gstate.perfect_join_executor.lock().unwrap();
                        *lock = Some(perfect_executor);
                    }
                }
            }
        }

        gstate.finalized.store(true, Ordering::Release);
        Ok(())
    }
}

impl fmt::Debug for HashJoin {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HashJoin")
            .field("join_type", &self.base.join.join_type)
            .field("conditions_count", &self.base.conditions.len())
            .field("types", &self.base.join.types)
            .finish()
    }
}

// ========== States ==========

///
/// Contains the global hash table and coordinates parallel build.
pub(super) struct HashJoinGlobalSinkState {
    /// Global HT used by the join.
    pub(super) hash_table: Arc<JoinHashTable>,

    /// Hash tables built by each thread.
    pub(super) local_hash_tables: Mutex<Vec<Arc<JoinHashTable>>>,

    /// Global filter pushdown state.
    pub filter_gstate: Option<JoinFilterGlobalState>,

    /// Build side column types.
    pub(super) build_types: Vec<LogicalType>,

    /// Join type.
    pub(super) join_type: JoinType,

    /// Whether or not the hash table has been finalized.
    pub(super) finalized: AtomicBool,

    /// The number of active local states.
    pub(super) active_local_states: AtomicUsize,

    /// Total number of threads for this sink.
    pub(super) num_threads: AtomicUsize,

    /// Runtime memory tracker for hash join build/external reservation.
    pub(super) memory_tracker: Arc<OperatorExternalMemoryTracker>,

    /// Shared build-side retained object spanning build/probe/source phases.
    pub(super) retained_object: Arc<SharedRetainedObject>,

    /// Finalize-time build/probe sizing stats.
    pub(super) total_size: AtomicUsize,
    pub(super) max_partition_size: AtomicUsize,
    pub(super) max_partition_count: AtomicUsize,
    pub(super) probe_side_requirement: AtomicUsize,

    /// Perfect hash join executor (optional).
    pub perfect_join_executor: Mutex<Option<PerfectHashJoinExecutor>>,
    /// Whether `prepare_finalize()` has completed.
    pub(super) finalize_prepared: AtomicBool,
    /// Whether the session explicitly requested external execution.
    pub(super) force_external: bool,
    /// Whether this operator externalized to spill path.
    pub(super) externalized: Arc<AtomicBool>,

    /// External hash join runtime.
    pub(super) external_runtime: Mutex<Option<external::HashJoinExternalRuntime>>,

    /// Source rows materialized during external SCAN_HT stage.
    pub(super) external_source_rows: Mutex<Option<RowStore>>,
    /// Shared scan progress for external source rows.
    pub(super) external_source_scan_state: Mutex<RowScanState>,

    /// Explicit external fallback reason, surfaced in EXPLAIN/profile.
    pub(super) external_fallback_reason: Mutex<Option<String>>,
}

impl HashJoinGlobalSinkState {
    fn new(
        hash_table: Arc<JoinHashTable>,
        build_types: Vec<LogicalType>,
        join_type: JoinType,
        memory_tracker: Arc<OperatorExternalMemoryTracker>,
        retained_object: Arc<SharedRetainedObject>,
        force_external: bool,
    ) -> Self {
        Self {
            hash_table,
            local_hash_tables: Mutex::new(Vec::new()),
            build_types,
            join_type,
            finalized: AtomicBool::new(false),
            active_local_states: AtomicUsize::new(0),
            num_threads: AtomicUsize::new(0),
            memory_tracker,
            retained_object,
            total_size: AtomicUsize::new(0),
            max_partition_size: AtomicUsize::new(0),
            max_partition_count: AtomicUsize::new(0),
            probe_side_requirement: AtomicUsize::new(0),
            perfect_join_executor: Mutex::new(None),
            filter_gstate: None,
            finalize_prepared: AtomicBool::new(false),
            force_external,
            externalized: Arc::new(AtomicBool::new(false)),
            external_runtime: Mutex::new(None),
            external_source_rows: Mutex::new(None),
            external_source_scan_state: Mutex::new(RowScanState::default()),
            external_fallback_reason: Mutex::new(None),
        }
    }

    /// Get count of rows in hash table.
    fn count(&self) -> usize {
        self.hash_table.count()
    }

    /// Prepare the sink state for finalize.
    fn prepare_finalize(&self) -> Result<()> {
        if self.finalize_prepared.swap(true, Ordering::AcqRel) {
            return Ok(());
        }

        let remaining = self.active_local_states.load(Ordering::Acquire);
        if remaining != 0 {
            return Err(paro_error::internal(format!(
                "HashJoin prepare_finalize called before all local states combined (remaining={remaining})"
            )));
        }

        Ok(())
    }

    pub(super) fn externalized(&self) -> bool {
        self.externalized.load(Ordering::Acquire)
    }

    fn peak_reservation(&self) -> u64 {
        self.memory_tracker.peak_bytes().unwrap_or(0) as u64
    }

    fn temp_storage_bytes(&self) -> u64 {
        self.total_size.load(Ordering::Acquire) as u64
    }

    pub(super) fn build_memory_context(&self) -> MemoryAccountingContext {
        let owner: Arc<dyn MemoryOwner> = self.retained_object.clone();
        MemoryAccountingContext::from_owner(
            owner,
            MemoryDomain::Host,
            HASH_JOIN_MEMORY_TAG,
            MemoryAccountingClass::Revocable,
        )
    }

    pub(super) fn has_capacity_for_total(&self, bytes: usize) -> Result<bool> {
        let current = self.memory_tracker.accounted_bytes()?;
        let additional = bytes.saturating_sub(current);
        self.memory_tracker
            .can_acquire_capacity(additional)
            .map_err(Into::into)
    }

    pub(super) fn cleanup_external_spill_state(&self, clear_source_rows: bool) {
        let runtime = self.external_runtime.lock().unwrap().take();
        drop(runtime);

        if clear_source_rows {
            *self.external_source_rows.lock().unwrap() = None;
        }
        self.external_source_scan_state.lock().unwrap().reset();

        self.hash_table.reset_runtime_state();
        self.hash_table.reset_data_collection();
    }

    fn cleanup_after_error(&self) {
        self.local_hash_tables.lock().unwrap().clear();
        self.externalized.store(false, Ordering::Release);
        self.finalized.store(false, Ordering::Release);
        self.cleanup_external_spill_state(true);
    }
}

#[derive(Debug)]
struct HashJoinBuildReclaimer {
    memory_tracker: Arc<OperatorExternalMemoryTracker>,
    externalized: Arc<AtomicBool>,
}

impl Reclaimer for HashJoinBuildReclaimer {
    fn name(&self) -> &str {
        "hash_join_build_side"
    }

    fn reclaimable_bytes(&self) -> usize {
        if self.externalized.load(Ordering::Acquire) {
            self.memory_tracker.accounted_bytes().unwrap_or(0)
        } else {
            0
        }
    }

    fn reclaim_sync(&self, target_bytes: usize) -> paro_common::memory::MemoryResult<ReclaimStats> {
        if !self.externalized.load(Ordering::Acquire) {
            return Ok(ReclaimStats::empty(target_bytes));
        }
        let reclaimed = self.memory_tracker.reclaim_accounted_bytes(target_bytes)?;
        Ok(ReclaimStats::new(target_bytes, reclaimed, reclaimed))
    }

    fn spill_cost(&self) -> SpillCost {
        SpillCost::AccountingRelease
    }
}

impl fmt::Debug for HashJoinGlobalSinkState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HashJoinGlobalSinkState")
            .field("row_count", &self.count())
            .field("finalized", &self.finalized.load(Ordering::Relaxed))
            .field(
                "finalize_prepared",
                &self.finalize_prepared.load(Ordering::Relaxed),
            )
            .field("build_types", &self.build_types)
            .field("join_type", &self.join_type)
            .finish()
    }
}

impl GlobalSinkState for HashJoinGlobalSinkState {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
    fn sink_state_name(&self) -> &str {
        "HashJoinGlobalSinkState"
    }
}

impl Drop for HashJoinGlobalSinkState {
    fn drop(&mut self) {
        let runtime = self
            .external_runtime
            .get_mut()
            .expect("hash join external runtime mutex should not be poisoned")
            .take();
        drop(runtime);

        *self
            .external_source_rows
            .get_mut()
            .expect("hash join external source rows mutex should not be poisoned") = None;
        self.external_source_scan_state
            .get_mut()
            .expect("hash join external source scan state mutex should not be poisoned")
            .reset();

        self.hash_table.reset_runtime_state();
        self.hash_table.reset_data_collection();
    }
}

impl GlobalSourceState for HashJoinGlobalSinkState {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

/// Local source state for hash join (probe / hash table scan).
pub(super) struct HashJoinLocalSourceState {
    /// State for scanning build-side tuples when emitting as source.
    pub(super) full_outer_scan_state:
        Option<crate::join_hashtable::join_hashtable::FullOuterScanState>,
}

impl fmt::Debug for HashJoinLocalSourceState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HashJoinLocalSourceState").finish()
    }
}

impl LocalSourceState for HashJoinLocalSourceState {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

/// Local sink state for hash join (build / insert into HT).
///
pub(super) struct HashJoinLocalSinkState {
    /// Thread-local hash table for parallel build.
    pub(super) hash_table: Option<Arc<JoinHashTable>>,
    /// Cached build-side key executors.
    pub(super) build_key_executors: Vec<ExpressionExecutor>,

    /// Local filter pushdown state.
    pub filter_lstate: Option<JoinFilterLocalState>,
}

impl fmt::Debug for HashJoinLocalSinkState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HashJoinLocalSinkState")
            .field("has_local_ht", &self.hash_table.is_some())
            .finish()
    }
}

impl LocalSinkState for HashJoinLocalSinkState {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

/// Global source state for hash join (probe).
///
pub(super) struct HashJoinGlobalSourceState {
    /// Reference to the finalized hash table.
    pub(super) hash_table: Arc<JoinHashTable>,

    /// Join type.
    pub(super) join_type: JoinType,
}

impl fmt::Debug for HashJoinGlobalSourceState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HashJoinGlobalSourceState")
            .field("join_type", &self.join_type)
            .field("row_count", &self.hash_table.count())
            .finish()
    }
}

impl GlobalSourceState for HashJoinGlobalSourceState {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

/// Operator state for hash join (probe).
pub(super) struct HashJoinOperatorState {
    /// State for iterating over hash table results.
    pub(super) scan_structure: crate::join_hashtable::scan_structure::ScanStructure,
    /// Chunks for probe keys.
    pub(super) probe_keys: Chunk,
    /// Whether the current input batch still has pending join results to emit.
    pub(super) probe_in_progress: bool,
    /// Current probe input batch (used when only a subset can be probed in external mode).
    pub(super) current_probe_input: Chunk,
    /// Per-thread probe spill append state in external mode.
    pub(super) external_probe_local_state: Option<ProbeSpillLocalState>,
    /// Per-thread scan state for replaying probe spill chunks.
    pub(super) external_probe_scan_state: RowScanState,
    /// Cached replay chunk for external probe.
    pub(super) external_probe_chunk: Chunk,
    /// Build-side scan state for SCAN_HT stage (external right/full variants).
    pub(super) external_build_scan_state:
        Option<crate::join_hashtable::join_hashtable::FullOuterScanState>,
    /// Reusable build chunk for SCAN_HT append.
    pub(super) external_build_chunk: Chunk,
    /// Cached probe-side key executors.
    pub(super) probe_key_executors: probe_engine::ProbeKeyExecutors,
    /// Cached residual-condition executors for probe-side filtering.
    pub(super) residual_condition_executors: Vec<probe_engine::ResidualConditionExecutors>,
}

impl fmt::Debug for HashJoinOperatorState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HashJoinOperatorState").finish()
    }
}

impl OperatorState for HashJoinOperatorState {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

// ========== PhysicalOperator Implementation ==========

impl PhysicalOperator for HashJoin {
    fn operator_type(&self) -> PhysicalOperatorType {
        PhysicalOperatorType::HashJoin
    }

    fn runtime_memory_stats(&self) -> ExplainRuntimeStats {
        HashJoin::runtime_memory_stats(self)
    }

    fn types(&self) -> &[LogicalType] {
        &self.base.join.types
    }

    fn explain_params(&self) -> Vec<String> {
        let mut params = vec![format!("Join Type: {}", self.base.join.join_type)];
        if !self.base.conditions.is_empty() {
            params.push(format!("Join Condition: {}", self.base.condition_info()));
        }
        let externalized = self
            .sink_state()
            .and_then(|sink_state| {
                sink_state
                    .as_any()
                    .downcast_ref::<HashJoinGlobalSinkState>()
                    .map(HashJoinGlobalSinkState::externalized)
            })
            .unwrap_or(false);
        params.push(format!("External: {externalized}"));
        if let Some(reason) = self.sink_state().and_then(|sink_state| {
            sink_state
                .as_any()
                .downcast_ref::<HashJoinGlobalSinkState>()
                .and_then(|sink| sink.external_fallback_reason.lock().unwrap().clone())
        }) {
            params.push(format!("External Fallback: {reason}"));
        }
        params
    }

    fn children_count(&self) -> usize {
        2
    }

    fn child(&self, index: usize) -> Option<&dyn PhysicalOperator> {
        match index {
            0 => Some(self.base.join.left.as_ref()),
            1 => Some(self.base.join.right.as_ref()),
            _ => None,
        }
    }

    fn child_arc(&self, index: usize) -> Option<Arc<dyn PhysicalOperator>> {
        match index {
            0 => Some(self.base.join.left.clone()),
            1 => Some(self.base.join.right.clone()),
            _ => None,
        }
    }

    fn is_source(&self) -> bool {
        self.base.join.is_source()
    }

    fn is_sink(&self) -> bool {
        true
    }

    fn parallel_sink(&self) -> bool {
        true
    }

    fn requires_final_execute(&self) -> bool {
        true
    }

    fn set_sink_state(&self, state: Arc<dyn GlobalSinkState>) {
        let mut lock = self.sink_state.lock().unwrap();
        *lock = Some(state);
    }

    fn sink_state(&self) -> Option<Arc<dyn GlobalSinkState>> {
        self.sink_state.lock().unwrap().clone()
    }

    fn get_operator_state(&self, ctx: &ExecutionContext) -> Result<Box<dyn OperatorState>> {
        let sink = self
            .sink_state()
            .ok_or_else(|| paro_error::internal("No sink".to_string()))?;
        let gsink = sink
            .as_any()
            .downcast_ref::<HashJoinGlobalSinkState>()
            .ok_or_else(|| paro_error::internal("Invalid sink".to_string()))?;

        let types: Vec<LogicalType> = self
            .base
            .equality_conditions
            .iter()
            .map(|c| c.left.return_type())
            .collect();
        Ok(Box::new(HashJoinOperatorState {
            scan_structure: gsink.hash_table.create_scan_structure()?,
            probe_keys: Chunk::try_init_empty(
                &types,
                ctx.allocator(paro_common::allocator::MemoryTag::BaseTable),
            )?,
            probe_in_progress: false,
            current_probe_input: Chunk::try_new(
                ctx.allocator(paro_common::allocator::MemoryTag::BaseTable),
            )?,
            external_probe_local_state: None,
            external_probe_scan_state: RowScanState::default(),
            external_probe_chunk: Chunk::try_new(
                ctx.allocator(paro_common::allocator::MemoryTag::BaseTable),
            )?,
            external_build_scan_state: None,
            external_build_chunk: Chunk::try_new(
                ctx.allocator(paro_common::allocator::MemoryTag::BaseTable),
            )?,
            probe_key_executors: probe_engine::ProbeKeyExecutors {
                executors: self
                    .base
                    .equality_conditions
                    .iter()
                    .map(|condition| ExpressionExecutor::new(&condition.left))
                    .collect(),
            },
            residual_condition_executors: self
                .build_layout
                .residual_conditions_on_build_payload
                .iter()
                .map(|condition| probe_engine::ResidualConditionExecutors {
                    left: ExpressionExecutor::new(&condition.left),
                    right: ExpressionExecutor::new(&condition.right),
                })
                .collect(),
        }))
    }

    // --- Sink (hash join build) ---

    /// Get the global sink state.
    fn get_global_sink_state(&self, ctx: &ExecutionContext) -> Result<Box<dyn GlobalSinkState>> {
        Self::validate_join_type(self.base.join.join_type)?;

        let force_external = ctx.force_external();
        let memory_tracker = Arc::new(OperatorExternalMemoryTracker::new(
            ctx.operator_memory_account(),
            MemoryDomain::Host,
            HASH_JOIN_MEMORY_TAG,
            HASH_JOIN_MEMORY_CLASS,
        ));
        let retained_owner: Arc<dyn MemoryOwner> = memory_tracker.clone();
        let retained_object = Arc::new(SharedRetainedObject::new(
            "hash_join_build_side",
            retained_owner,
            MemoryDomain::Host,
            HASH_JOIN_MEMORY_TAG,
        ));
        let hash_table = self.initialize_hash_table(ctx, retained_object.clone());

        let mut gstate = HashJoinGlobalSinkState::new(
            hash_table,
            self.build_layout.build_payload_types.clone(),
            self.base.join.join_type,
            memory_tracker.clone(),
            retained_object,
            force_external,
        );
        let reclaimer: Arc<dyn Reclaimer> = Arc::new(HashJoinBuildReclaimer {
            memory_tracker,
            externalized: gstate.externalized.clone(),
        });
        ctx.query_memory_pool().register_reclaimer(reclaimer);

        if let Some(ref filter_info) = self.filter_pushdown {
            gstate.filter_gstate = Some(filter_info.get_global_state());
        }

        Ok(Box::new(gstate))
    }

    /// Get the local sink state.
    fn get_local_sink_state(&self, ctx: &ExecutionContext) -> Result<Box<dyn LocalSinkState>> {
        let sink = self.sink_state().ok_or_else(|| {
            paro_error::internal("HashJoin local sink requires global sink state")
        })?;
        let gstate = sink
            .as_any()
            .downcast_ref::<HashJoinGlobalSinkState>()
            .ok_or_else(|| paro_error::internal("Invalid global sink state".to_string()))?;
        gstate.active_local_states.fetch_add(1, Ordering::AcqRel);
        gstate.num_threads.fetch_add(1, Ordering::Relaxed);

        let local_ht = Arc::new(JoinHashTable::new_with_memory(
            ctx.buffer_pool().clone(),
            ctx.allocator(paro_common::allocator::MemoryTag::BaseTable),
            self.base.equality_conditions.clone(),
            self.build_layout.build_payload_types.clone(),
            self.base.join.join_type,
            JoinHashTableConfig::default(),
            gstate.build_memory_context(),
        ));

        let mut lstate = HashJoinLocalSinkState {
            hash_table: Some(local_ht),
            build_key_executors: self
                .base
                .equality_conditions
                .iter()
                .map(|condition| ExpressionExecutor::new(&condition.right))
                .collect(),
            filter_lstate: None,
        };

        if let Some(ref filter_info) = self.filter_pushdown {
            lstate.filter_lstate = Some(filter_info.get_local_state());
        }

        Ok(Box::new(lstate))
    }

    /// Sink a chunk into the hash table.
    fn sink(
        &self,
        ctx: &ExecutionContext,
        chunk: &Chunk,
        input: &mut OperatorSinkInput,
    ) -> Result<SinkResultType> {
        ctx.check_cancelled()?;
        let lstate = input
            .local_state
            .as_any_mut()
            .downcast_mut::<HashJoinLocalSinkState>()
            .ok_or_else(|| paro_error::internal("Invalid local sink state".to_string()))?;

        // Extract join keys from the chunk
        let mut key_chunks = Vec::with_capacity(self.base.equality_conditions.len());
        for (cond, executor) in self
            .base
            .equality_conditions
            .iter()
            .zip(lstate.build_key_executors.iter_mut())
        {
            let vec = executor.execute_expression(0, chunk, None, chunk.size(), ctx)?;
            key_chunks.push(Self::materialize_key_vector(
                ctx,
                vec,
                cond.right.return_type(),
                chunk.size(),
            )?);
        }
        let key_chunk = Chunk::from_arc_vectors(key_chunks, chunk.allocator().clone());

        // Extract payload (projection/residual-driven build columns).
        let payload_chunk = self.build_payload_chunk(chunk)?;

        // Build into the local hash table
        if let Some(ref local_ht) = lstate.hash_table {
            local_ht.build(&key_chunk, &payload_chunk)?;
        }

        // Sink into filter pushdown
        if let Some(ref filter_info) = self.filter_pushdown {
            if let Some(ref mut filter_lstate) = lstate.filter_lstate {
                filter_info.sink(&key_chunk, filter_lstate);
            }
        }

        Ok(SinkResultType::NeedMoreInput)
    }

    /// Combine local state into global state.
    fn combine(
        &self,
        _ctx: &ExecutionContext,
        input: &mut OperatorSinkCombineInput,
    ) -> Result<SinkCombineResultType> {
        let gstate = input
            .global_state
            .as_any()
            .downcast_ref::<HashJoinGlobalSinkState>()
            .ok_or_else(|| paro_error::internal("Invalid global sink state".to_string()))?;

        let lstate = input
            .local_state
            .as_any_mut()
            .downcast_mut::<HashJoinLocalSinkState>()
            .ok_or_else(|| paro_error::internal("Invalid local sink state".to_string()))?;

        if let Some(local_ht) = lstate.hash_table.take() {
            gstate.local_hash_tables.lock().unwrap().push(local_ht);
        }

        // Combine filter states
        if let Some(ref filter_info) = self.filter_pushdown {
            if let (Some(ref g_filter), Some(l_filter)) =
                (&gstate.filter_gstate, lstate.filter_lstate.take())
            {
                filter_info.combine(g_filter, l_filter);
            }
        }

        let mut remaining = gstate.active_local_states.load(Ordering::Acquire);
        loop {
            if remaining == 0 {
                return Err(paro_error::internal(
                    "HashJoin combine called with no active local states".to_string(),
                ));
            }
            match gstate.active_local_states.compare_exchange(
                remaining,
                remaining - 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(actual) => remaining = actual,
            }
        }

        Ok(SinkCombineResultType::Finished)
    }

    fn prepare_finalize(&self, gstate: &dyn GlobalSinkState) -> Result<()> {
        let gstate = gstate
            .as_any()
            .downcast_ref::<HashJoinGlobalSinkState>()
            .ok_or_else(|| paro_error::internal("Invalid global sink state".to_string()))?;
        gstate.prepare_finalize()?;

        let local_tables = gstate.local_hash_tables.lock().unwrap();
        let mut total_data_size = 0usize;
        let mut total_count = 0usize;
        let mut max_partition_size = 0usize;
        let mut max_partition_count = 0usize;
        let mut max_partition_ht_size = 0usize;
        for local_ht in local_tables.iter() {
            let partition_size = local_ht.build_rows_size_in_bytes();
            let partition_count = local_ht.count();
            total_data_size = total_data_size.saturating_add(partition_size);
            total_count = total_count.saturating_add(partition_count);

            let partition_ht_size =
                JoinHashTable::estimate_total_size(partition_size, partition_count);
            if partition_ht_size > max_partition_ht_size {
                max_partition_ht_size = partition_ht_size;
                max_partition_size = partition_size;
                max_partition_count = partition_count;
            }
        }
        drop(local_tables);

        let total_size = JoinHashTable::estimate_total_size(total_data_size, total_count);
        let num_threads = gstate
            .num_threads
            .load(Ordering::Relaxed)
            .max(1)
            .min(HASH_JOIN_EXTERNAL_PARTITION_THREAD_CAP);
        let probe_side_requirement = external::get_partitioning_space_requirement(
            self.base.join.left.types(),
            HASH_JOIN_INITIAL_RADIX_BITS,
            num_threads,
        );

        let max_partition_ht_size = max_partition_size.saturating_add(
            JoinHashTable::pointer_table_size_for_count(max_partition_count),
        );
        gstate.memory_tracker.set_minimum_reservation_bytes(
            max_partition_ht_size.saturating_add(probe_side_requirement),
        )?;
        let _full_target_available = gstate.has_capacity_for_total(total_size)?;

        gstate.total_size.store(total_size, Ordering::Release);
        gstate
            .max_partition_size
            .store(max_partition_size, Ordering::Release);
        gstate
            .max_partition_count
            .store(max_partition_count, Ordering::Release);
        gstate
            .probe_side_requirement
            .store(probe_side_requirement, Ordering::Release);
        Ok(())
    }

    fn finalize(&self, input: &OperatorSinkFinalizeInput) -> Result<SinkFinalizeType> {
        let gstate = input
            .global_state
            .as_any()
            .downcast_ref::<HashJoinGlobalSinkState>()
            .ok_or_else(|| paro_error::internal("Invalid global sink state".to_string()))?;
        let result = (|| {
            self.prepare_finalize(input.global_state)?;

            if gstate.finalized.load(Ordering::Acquire) {
                if gstate.hash_table.count() == 0 && self.base.join.empty_result_if_rhs_is_empty() {
                    return Ok(SinkFinalizeType::NoOutputPossible);
                }
                return Ok(SinkFinalizeType::Ready);
            }

            let mut total_size = gstate.total_size.load(Ordering::Acquire);
            let probe_side_requirement = gstate.probe_side_requirement.load(Ordering::Acquire);
            let force_external = gstate.force_external;
            *gstate.external_fallback_reason.lock().unwrap() = None;
            let mut full_target_available = gstate.has_capacity_for_total(total_size)?;
            let mut external = (force_external && total_size > 0) || !full_target_available;

            if external && !force_external {
                let local_tables = gstate.local_hash_tables.lock().unwrap();
                let mut total_data_size = 0usize;
                let mut total_count = 0usize;
                for local_ht in local_tables.iter() {
                    total_data_size =
                        total_data_size.saturating_add(local_ht.build_rows_size_in_bytes());
                    total_count = total_count.saturating_add(local_ht.count());
                }
                drop(local_tables);

                let lowered_size = external::estimate_total_size_with_load_factor(
                    total_data_size,
                    total_count,
                    HASH_JOIN_EXTERNAL_LOAD_FACTOR,
                );
                if gstate.has_capacity_for_total(lowered_size)? {
                    total_size = lowered_size;
                    external = false;
                    full_target_available = true;
                    gstate.total_size.store(total_size, Ordering::Release);
                    gstate
                        .memory_tracker
                        .set_minimum_reservation_bytes(total_size.max(1))?;
                }
            }

            if !external && full_target_available {
                self.finalize_in_memory_hash_table(gstate)?;
                gstate.externalized.store(false, Ordering::Release);
                *gstate.external_runtime.lock().unwrap() = None;
                *gstate.external_source_rows.lock().unwrap() = None;
                gstate.external_source_scan_state.lock().unwrap().reset();
                if gstate.hash_table.count() == 0 && self.base.join.empty_result_if_rhs_is_empty() {
                    return Ok(SinkFinalizeType::NoOutputPossible);
                }
                return Ok(SinkFinalizeType::Ready);
            }

            if !gstate.hash_table.buffer_pool().has_temporary_directory() {
                let message = if force_external {
                    "force_external requires a temporary directory (SET temp_directory)"
                } else {
                    "hash join externalization requires a temporary directory (SET temp_directory)"
                };
                return Err(paro_error::invalid_input(message));
            }

            let local_tables = {
                let mut tables = gstate.local_hash_tables.lock().unwrap();
                std::mem::take(&mut *tables)
            };
            let hash_col_idx = gstate.hash_table.hash_column_index();
            let layout = Arc::new(gstate.hash_table.layout().clone());
            let mut radix_bits = HASH_JOIN_INITIAL_RADIX_BITS;
            let mut sink_builder = RadixPartitionedRowsBuilder::new_with_memory(
                gstate.hash_table.buffer_pool().clone(),
                layout.clone(),
                MemoryTag::HashTable,
                radix_bits,
                hash_col_idx,
                gstate.build_memory_context(),
            )?;
            let global_has_null = external::repartition_local_tables_into_sink_collection(
                &local_tables,
                &mut sink_builder,
            )?;
            let mut sink_collection = sink_builder.seal();
            drop(local_tables);

            let recompute_partition_stats =
                |collection: &paro_storage::row::RadixPartitionedRows| -> (usize, usize, usize, usize, usize) {
                    let mut total_data = 0usize;
                    let mut total_rows = 0usize;
                    let mut max_size = 0usize;
                    let mut max_count = 0usize;
                    for partition in collection.partitions() {
                        let size = partition.size_in_bytes();
                        let count = partition.count() as usize;
                        total_data = total_data.saturating_add(size);
                        total_rows = total_rows.saturating_add(count);
                        let part_ht = JoinHashTable::estimate_total_size(size, count);
                        let current_max = JoinHashTable::estimate_total_size(max_size, max_count);
                        if part_ht > current_max {
                            max_size = size;
                            max_count = count;
                        }
                    }
                    let total_ht = JoinHashTable::estimate_total_size(total_data, total_rows);
                    let max_ht = JoinHashTable::estimate_total_size(max_size, max_count);
                    (total_ht, total_data, total_rows, max_size, max_ht)
                };

            let (
                mut total_ht_size,
                _total_data,
                _total_rows,
                mut max_partition_size,
                mut max_partition_ht_size,
            ) = recompute_partition_stats(&sink_collection);
            let repartition_budget = gstate.memory_tracker.minimum_reservation_bytes()?.max(1);
            let mut max_partition_count = if max_partition_size == 0 {
                0
            } else {
                sink_collection
                    .partitions()
                    .iter()
                    .find(|partition| partition.size_in_bytes() == max_partition_size)
                    .map(|partition| partition.count() as usize)
                    .unwrap_or(0)
            };
            let mut very_very_skewed = max_partition_ht_size as f64
                >= (HASH_JOIN_SKEW_THRESHOLD * (total_ht_size.max(1) as f64));

            while !very_very_skewed
                && max_partition_ht_size.saturating_add(probe_side_requirement) > repartition_budget
                && radix_bits < HASH_JOIN_MAX_RADIX_BITS
            {
                let new_bits = radix_bits.saturating_add(1);
                sink_collection = sink_collection.into_repartitioned(new_bits)?;
                radix_bits = new_bits;

                let (
                    new_total_ht_size,
                    _new_total_data,
                    _new_total_rows,
                    new_max_partition_size,
                    new_max_partition_ht_size,
                ) = recompute_partition_stats(&sink_collection);
                total_ht_size = new_total_ht_size;
                max_partition_size = new_max_partition_size;
                max_partition_ht_size = new_max_partition_ht_size;
                max_partition_count = if new_max_partition_size == 0 {
                    0
                } else {
                    sink_collection
                        .partitions()
                        .iter()
                        .find(|partition| partition.size_in_bytes() == new_max_partition_size)
                        .map(|partition| partition.count() as usize)
                        .unwrap_or(0)
                };
                very_very_skewed = max_partition_ht_size as f64
                    >= (HASH_JOIN_SKEW_THRESHOLD * (total_ht_size.max(1) as f64));
            }

            total_size = total_ht_size;
            gstate.total_size.store(total_size, Ordering::Release);
            gstate
                .max_partition_size
                .store(max_partition_size, Ordering::Release);
            gstate
                .max_partition_count
                .store(max_partition_count, Ordering::Release);

            let min_reservation = max_partition_size
                .saturating_add(JoinHashTable::pointer_table_size_for_count(
                    max_partition_count,
                ))
                .saturating_add(probe_side_requirement);
            gstate
                .memory_tracker
                .set_minimum_reservation_bytes(min_reservation.max(1))?;
            let _total_target_available = gstate.has_capacity_for_total(total_size.max(1))?;

            let mut probe_types = self.base.join.left.types().to_vec();
            probe_types.push(LogicalType::UBigInt);
            let probe_spill = ProbeSpill::new(
                gstate.hash_table.buffer_pool().clone(),
                probe_types,
                radix_bits,
                self.base.join.left.types().len(),
            )?;

            let partition_count = sink_collection.partition_count();
            let mut runtime = external::HashJoinExternalRuntime {
                radix_bits,
                sink_collection,
                completed_partitions: vec![false; partition_count],
                current_partitions: vec![false; partition_count],
                probe_spill,
                probe_spill_finalized: false,
                probe_rows: None,
                probe_stage: external::HashJoinExternalProbeStage::PrepareBuild,
                global_has_null,
                probe_side_requirement,
                source_output_rows_builder: None,
            };

            let prepared = external::prepare_external_build_round(self, gstate, &mut runtime)?;
            if prepared {
                runtime.probe_stage = external::HashJoinExternalProbeStage::Probe;
            } else {
                runtime.probe_stage = external::HashJoinExternalProbeStage::Done;
            }

            *gstate.external_runtime.lock().unwrap() = Some(runtime);
            *gstate.external_source_rows.lock().unwrap() = None;
            gstate.external_source_scan_state.lock().unwrap().reset();
            gstate.externalized.store(true, Ordering::Release);
            gstate.finalized.store(true, Ordering::Release);

            if gstate.hash_table.count() == 0 && self.base.join.empty_result_if_rhs_is_empty() {
                return Ok(SinkFinalizeType::NoOutputPossible);
            }

            Ok(SinkFinalizeType::Ready)
        })();

        if result.is_err() {
            gstate.cleanup_after_error();
        }

        result
    }

    // --- Source (hash join probe) ---

    /// Get the global source state.
    fn get_global_source_state(
        &self,
        _ctx: &ExecutionContext,
        sink_state: Option<&dyn GlobalSinkState>,
    ) -> Result<Box<dyn GlobalSourceState>> {
        Self::validate_join_type(self.base.join.join_type)?;

        // Helper function to create source state from sink state
        fn create_source_state(
            sink: &HashJoinGlobalSinkState,
        ) -> Result<Box<dyn GlobalSourceState>> {
            sink.retained_object.rebind_reclaimer();
            Ok(Box::new(HashJoinGlobalSourceState {
                hash_table: Arc::clone(&sink.hash_table),
                join_type: sink.join_type,
            }))
        }

        // Case 1: Use provided sink_state
        if let Some(sink) = sink_state {
            let hash_join_sink = sink
                .as_any()
                .downcast_ref::<HashJoinGlobalSinkState>()
                .ok_or_else(|| {
                    paro_error::internal(format!(
                        "Invalid sink state type for HashJoin. Expected HashJoinGlobalSinkState, got: {}",
                        sink.sink_state_name()
                    ))
                })?;
            return create_source_state(hash_join_sink);
        }

        // Case 2: Fall back to internally stored state
        let internal_sink = self.sink_state().ok_or_else(|| {
            paro_error::internal("HashJoin requires sink state when used as source".to_string())
        })?;

        let hash_join_sink = internal_sink
            .as_any()
            .downcast_ref::<HashJoinGlobalSinkState>()
            .ok_or_else(|| {
                paro_error::internal(format!(
                    "Invalid internal sink state type for HashJoin. Expected HashJoinGlobalSinkState, got: {}",
                    internal_sink.sink_state_name()
                ))
            })?;

        create_source_state(hash_join_sink)
    }

    /// Get the local source state.
    /// Get the local source state.
    fn get_local_source_state(
        &self,
        _ctx: &ExecutionContext,
        gstate: &dyn GlobalSourceState,
    ) -> Result<Box<dyn LocalSourceState>> {
        let full_outer_scan_state = gstate
            .as_any()
            .downcast_ref::<HashJoinGlobalSourceState>()
            .and_then(|state| {
                state
                    .hash_table
                    .has_found_flag()
                    .then(|| state.hash_table.create_full_outer_scan_state())
            });

        Ok(Box::new(HashJoinLocalSourceState {
            full_outer_scan_state,
        }))
    }

    fn execute(
        &self,
        ctx: &ExecutionContext,
        input: &Chunk,
        chunk: &mut Chunk,
        _gstate: &dyn GlobalOperatorState,
        state: &mut dyn OperatorState,
        _memory: crate::memory_runtime::OperatorMemoryScope<'_>,
    ) -> Result<crate::result_type::OperatorResultType> {
        Self::validate_join_type(self.base.join.join_type)?;

        use crate::result_type::OperatorResultType;

        let state = state
            .as_any_mut()
            .downcast_mut::<HashJoinOperatorState>()
            .ok_or_else(|| paro_error::internal("Invalid operator state".to_string()))?;

        let sink = self
            .sink_state()
            .ok_or_else(|| paro_error::internal("HashJoin requires sink state".to_string()))?;
        let gsink = sink
            .as_any()
            .downcast_ref::<HashJoinGlobalSinkState>()
            .ok_or_else(|| paro_error::internal("Invalid global sink state".to_string()))?;
        let ht = &gsink.hash_table;

        if !gsink.finalized.load(Ordering::Acquire) {
            return Ok(OperatorResultType::NeedMoreInput);
        }

        // Initialize output chunk
        probe_engine::prepare_output_chunk(chunk, &self.base.join.types, input.capacity())?;

        if ht.is_empty() {
            if self.base.join.empty_result_if_rhs_is_empty() {
                chunk.try_set_cardinality(0)?;
            } else {
                self.base.construct_empty_join_result(
                    input,
                    chunk,
                    ht.has_null.load(Ordering::Relaxed),
                )?;
            }
            return Ok(OperatorResultType::NeedMoreInput);
        }

        let externalized = gsink.externalized();

        // 1. Continue emitting rows for the current probe batch before re-probing.
        if state.probe_in_progress {
            let probe_input = if state.current_probe_input.size() > 0 {
                &state.current_probe_input
            } else {
                input
            };
            let count = probe_engine::scan_join_results(
                ctx,
                self.base.join.join_type,
                &state.probe_keys,
                probe_input,
                chunk,
                &mut state.scan_structure,
                ht,
                &self.base.join.left_projection_map,
                &self.build_layout.right_projection_map_for_build,
                &self.build_layout.residual_conditions_on_build_payload,
                &mut state.residual_condition_executors,
                &self.build_layout.build_payload_types,
            )?;
            state.probe_in_progress = !state.scan_structure.finished;
            if state.scan_structure.finished {
                state
                    .current_probe_input
                    .try_reset(state.current_probe_input.allocator().clone())?;
            }
            return Ok(probe_engine::result_for_probe_batch(
                count,
                state.scan_structure.finished,
            ));
        }

        if input.size() == 0 {
            chunk.try_set_cardinality(0)?;
            return Ok(OperatorResultType::NeedMoreInput);
        }

        // 2. Resolve probe keys for a new input batch.
        state.probe_keys = probe_engine::evaluate_probe_keys(
            ctx,
            input,
            &self.base.equality_conditions,
            &mut state.probe_key_executors,
        )?;
        state
            .current_probe_input
            .try_reset(state.current_probe_input.allocator().clone())?;

        // 3. Apply filter pushdown (disabled on external path to avoid
        // partition-at-a-time correctness corner cases with deferred probe).
        let mut sel = SelectionVector::try_incremental(input.size(), input.allocator().clone())?;
        let mut filtered_count = input.size();
        if !externalized {
            if let Some(ref filter_info) = self.filter_pushdown {
                if let Some(ref g_filter) = gsink.filter_gstate {
                    filtered_count =
                        filter_info.apply_filters(g_filter, &state.probe_keys, &mut sel);
                }
            }
        }

        // 4. Try perfect hash join (in-memory path only)
        if !externalized {
            let perfect_lock = gsink.perfect_join_executor.lock().unwrap();
            if let Some(perfect_executor) = perfect_lock.as_ref() {
                if filtered_count > 0 {
                    perfect_executor.probe(&state.probe_keys, input, chunk, Some(&sel))?;
                } else {
                    chunk.try_set_cardinality(0)?;
                }

                return Ok(OperatorResultType::NeedMoreInput);
            }
        }

        if externalized {
            return external::execute_external_probe(ctx, self, gsink, state, input, chunk);
        }

        // 5. Probe the hash table (in-memory path)
        if filtered_count > 0 {
            ht.probe(
                &state.probe_keys,
                &mut state.scan_structure,
                Some(&sel),
                filtered_count,
            )?;
            state.probe_in_progress = true;
        } else {
            // Filter pushdown can prove "no matches", but some join shapes still need
            // to emit unmatched probe-side rows (e.g. MARK/LEFT/ANTI/SINGLE).
            state.scan_structure.reset();
            state.probe_in_progress = false;
        }

        // 6. Get first batch of results
        let count = probe_engine::scan_join_results(
            ctx,
            self.base.join.join_type,
            &state.probe_keys,
            input,
            chunk,
            &mut state.scan_structure,
            ht,
            &self.base.join.left_projection_map,
            &self.build_layout.right_projection_map_for_build,
            &self.build_layout.residual_conditions_on_build_payload,
            &mut state.residual_condition_executors,
            &self.build_layout.build_payload_types,
        )?;
        state.probe_in_progress = !state.scan_structure.finished;
        if state.scan_structure.finished {
            state
                .current_probe_input
                .try_reset(state.current_probe_input.allocator().clone())?;
        }
        Ok(probe_engine::result_for_probe_batch(
            count,
            state.scan_structure.finished,
        ))
    }

    fn final_execute(
        &self,
        ctx: &ExecutionContext,
        chunk: &mut Chunk,
        _gstate: &dyn GlobalOperatorState,
        state: &mut dyn OperatorState,
        _memory: crate::memory_runtime::OperatorMemoryScope<'_>,
    ) -> Result<OperatorFinalizeResultType> {
        let state = state
            .as_any_mut()
            .downcast_mut::<HashJoinOperatorState>()
            .ok_or_else(|| paro_error::internal("Invalid operator state".to_string()))?;

        let sink = self
            .sink_state()
            .ok_or_else(|| paro_error::internal("HashJoin requires sink state".to_string()))?;
        let gsink = sink
            .as_any()
            .downcast_ref::<HashJoinGlobalSinkState>()
            .ok_or_else(|| paro_error::internal("Invalid global sink state".to_string()))?;
        if !gsink.externalized() {
            return Ok(OperatorFinalizeResultType::Finished);
        }
        external::drive_external_replay(ctx, self, gsink, state, chunk)
    }

    /// Get data from the hash join (probe path).
    fn get_data(
        &self,
        _ctx: &ExecutionContext,
        chunk: &mut Chunk,
        input: &mut OperatorSourceInput,
    ) -> Result<SourceResultType> {
        Self::validate_join_type(self.base.join.join_type)?;

        let gstate = input
            .global_state
            .as_any()
            .downcast_ref::<HashJoinGlobalSourceState>()
            .ok_or_else(|| paro_error::internal("Invalid global source state".to_string()))?;
        let lstate = input
            .local_state
            .as_any_mut()
            .downcast_mut::<HashJoinLocalSourceState>()
            .ok_or_else(|| paro_error::internal("Invalid local source state".to_string()))?;

        if let Some(sink) = self.sink_state() {
            if let Some(gsink) = sink.as_any().downcast_ref::<HashJoinGlobalSinkState>() {
                if gsink.externalized() {
                    return external::get_data_external(self, gsink, lstate, chunk);
                }
            }
        }

        let Some(scan_state) = lstate.full_outer_scan_state.as_mut() else {
            chunk.try_set_cardinality(0)?;
            return Ok(SourceResultType::Finished);
        };

        let emit_found = match self.base.join.join_type {
            JoinType::RightSemi => true,
            JoinType::Right | JoinType::Outer | JoinType::RightAnti => false,
            _ => {
                chunk.try_set_cardinality(0)?;
                return Ok(SourceResultType::Finished);
            }
        };

        probe_engine::prepare_output_chunk(
            chunk,
            &self.base.join.types,
            paro_common::vector::VECTOR_SIZE,
        )?;

        let mut build_chunk = Chunk::try_initialize(
            &self.build_layout.build_payload_types,
            paro_common::vector::VECTOR_SIZE,
            chunk.allocator().clone(),
        )?;
        let count = gstate
            .hash_table
            .scan_full_outer(scan_state, emit_found, &mut build_chunk)?;
        if count == 0 {
            chunk.try_set_cardinality(0)?;
            return Ok(SourceResultType::Finished);
        }

        let build_sel = SelectionVector::try_incremental(count, chunk.allocator().clone())?;
        match self.base.join.join_type {
            JoinType::Right | JoinType::Outer => construct_right_outer_scan_result(
                &build_chunk,
                &build_sel,
                count,
                &self.base.join.left_output_types,
                &self.build_layout.right_projection_map_for_build,
                chunk,
            ),
            JoinType::RightSemi | JoinType::RightAnti => construct_semi_join_result(
                &build_chunk,
                &build_sel,
                count,
                &self.build_layout.right_projection_map_for_build,
                chunk,
            ),
            _ => unreachable!("source path is only used by right/full/right-semi/right-anti"),
        }?;

        Ok(SourceResultType::HaveMoreOutput)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn build_pipelines(
        &self,
        op: &Arc<dyn PhysicalOperator>,
        current: &Arc<Pipeline>,
        meta_pipeline: &Arc<MetaPipeline>,
        state: &mut PipelineBuildState,
    ) {
        self.base
            .join
            .build_join_pipelines(op, current, meta_pipeline, state, true);
    }
}

#[cfg(test)]
mod tests {
    use super::{HashJoin, HashJoinGlobalSinkState, HashJoinGlobalSourceState};
    use std::fs;
    use std::sync::atomic::Ordering;
    use std::sync::{Arc, Mutex, OnceLock};
    use std::time::{SystemTime, UNIX_EPOCH};

    use paro_common::chunk::Chunk;
    use paro_common::memory::{MemoryDomain, MemoryOwner};
    use paro_common::runtime_value::Value;
    use paro_common::types::LogicalType;

    use paro_context::{RuntimeLimits, StatementContext, TestStatementContextBuilder};
    use paro_planner::expression::{
        ConstantExpression, Expression, OperatorExpression, OperatorType, ReferenceExpression,
    };
    use paro_planner::operator::join::{JoinComparisonType, JoinCondition, JoinType};
    use paro_scheduler::task::InterruptState;
    use paro_storage::buffer::BufferPool;

    use crate::execution_context::ExecutionContext;
    use crate::join_hashtable::join_hashtable::{JoinHashTable, JoinHashTableConfig};
    use crate::memory_runtime::{
        OperatorExternalMemoryTracker, OperatorMemoryAccount, QueryMemoryPool, SharedRetainedObject,
    };
    use crate::operator::projection::Projection;
    use crate::operator::scan::dummy_scan::PhysicalDummyScan;
    use crate::operator::scan::expression_scan::PhysicalExpressionScan;
    use crate::operator::state::{
        EmptyGlobalOperatorState, GlobalSinkState, OperatorSinkCombineInput,
        OperatorSinkFinalizeInput, OperatorSinkInput, OperatorSourceInput,
    };
    use crate::operator::PhysicalOperator;
    use crate::pipeline::build_state::PipelineBuildState;
    use crate::pipeline::meta_pipeline::{MetaPipeline, MetaPipelineType};
    use crate::query_executor::compiled::{CompiledStatement, ResultColumnDesc};
    use crate::query_executor::executor::Executor;
    use crate::result_type::{
        OperatorFinalizeResultType, OperatorResultType, SinkFinalizeType, SourceResultType,
    };
    use crate::thread_context::ThreadContext;

    fn constant_i32(value: i32) -> Expression {
        Expression::Constant(ConstantExpression::new(
            Value::Integer(value),
            LogicalType::Integer,
        ))
    }

    fn equality_condition() -> JoinCondition {
        JoinCondition::new(constant_i32(1), constant_i32(1), JoinComparisonType::Equal)
    }

    fn not_distinct_reference_condition() -> JoinCondition {
        JoinCondition::new(
            reference_i32(0),
            reference_i32(0),
            JoinComparisonType::NotDistinctFrom,
        )
    }

    fn reference_i32(index: usize) -> Expression {
        Expression::Reference(ReferenceExpression::new(index, LogicalType::Integer))
    }

    fn create_test_buffer_pool() -> Arc<BufferPool> {
        BufferPool::new_arc(64 * 1024 * 1024)
    }

    fn create_test_memory_tracker() -> Arc<OperatorExternalMemoryTracker> {
        let pool = Arc::new(QueryMemoryPool::unbounded());
        let account = Arc::new(OperatorMemoryAccount::new(pool));
        Arc::new(OperatorExternalMemoryTracker::new(
            account,
            MemoryDomain::Host,
            super::HASH_JOIN_MEMORY_TAG,
            super::HASH_JOIN_MEMORY_CLASS,
        ))
    }

    fn create_test_global_sink_state(
        hash_table: Arc<JoinHashTable>,
        build_types: Vec<LogicalType>,
        join_type: JoinType,
    ) -> Arc<HashJoinGlobalSinkState> {
        let memory_tracker = create_test_memory_tracker();
        let retained_owner: Arc<dyn MemoryOwner> = memory_tracker.clone();
        let retained_object = Arc::new(SharedRetainedObject::new(
            "test_hash_join_build_side",
            retained_owner,
            MemoryDomain::Host,
            super::HASH_JOIN_MEMORY_TAG,
        ));
        Arc::new(HashJoinGlobalSinkState::new(
            hash_table,
            build_types,
            join_type,
            memory_tracker,
            retained_object,
            false,
        ))
    }

    fn create_test_session() -> Arc<StatementContext> {
        TestStatementContextBuilder::minimal().build()
    }

    fn create_test_temp_dir(prefix: &str) -> String {
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock should be after unix epoch")
            .as_nanos();
        let path = std::env::temp_dir().join(format!("{prefix}_{}_{}", std::process::id(), suffix));
        fs::create_dir_all(&path).expect("test temp directory should be created");
        path.to_string_lossy().to_string()
    }

    fn create_force_external_session(max_memory: usize) -> (Arc<StatementContext>, String) {
        let temp_dir = create_test_temp_dir("paro_hash_join_external");
        let session = TestStatementContextBuilder::minimal()
            .with_limits(RuntimeLimits {
                max_threads: 1,
                max_memory,
                use_temporary_directory: true,
                temporary_directory: temp_dir.clone(),
                max_temp_directory_size: None,
                force_external: true,
            })
            .build();
        session.buffer_pool().set_memory_limit(max_memory).unwrap();
        session
            .buffer_pool()
            .set_temporary_directory(temp_dir.clone())
            .expect("temp directory should be configured");
        (session, temp_dir)
    }

    fn spill_file_test_guard() -> std::sync::MutexGuard<'static, ()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(())).lock().unwrap()
    }

    fn create_test_join(join_type: JoinType) -> HashJoin {
        let left = Arc::new(PhysicalDummyScan::with_types(vec![LogicalType::Integer]))
            as Arc<dyn PhysicalOperator>;
        let right = Arc::new(PhysicalDummyScan::with_types(vec![LogicalType::Integer]))
            as Arc<dyn PhysicalOperator>;

        HashJoin::new(
            left,
            right,
            join_type,
            vec![equality_condition()],
            vec![],
            vec![],
        )
        .expect("hash join should be created")
    }

    fn create_column_ref_test_join(join_type: JoinType) -> HashJoin {
        let left = Arc::new(PhysicalDummyScan::with_types(vec![LogicalType::Integer]))
            as Arc<dyn PhysicalOperator>;
        let right = Arc::new(PhysicalDummyScan::with_types(vec![LogicalType::Integer]))
            as Arc<dyn PhysicalOperator>;

        HashJoin::new(
            left,
            right,
            join_type,
            vec![JoinCondition::new(
                reference_i32(0),
                reference_i32(0),
                JoinComparisonType::Equal,
            )],
            vec![],
            vec![],
        )
        .expect("hash join should be created")
    }

    fn create_not_distinct_reference_test_join(join_type: JoinType) -> HashJoin {
        let left = Arc::new(PhysicalDummyScan::with_types(vec![LogicalType::Integer]))
            as Arc<dyn PhysicalOperator>;
        let right = Arc::new(PhysicalDummyScan::with_types(vec![LogicalType::Integer]))
            as Arc<dyn PhysicalOperator>;

        HashJoin::new(
            left,
            right,
            join_type,
            vec![not_distinct_reference_condition()],
            vec![],
            vec![],
        )
        .expect("hash join should be created")
    }

    fn create_residual_reference_test_join(join_type: JoinType) -> HashJoin {
        let left = Arc::new(PhysicalDummyScan::with_types(vec![LogicalType::Integer]))
            as Arc<dyn PhysicalOperator>;
        let right = Arc::new(PhysicalDummyScan::with_types(vec![LogicalType::Integer]))
            as Arc<dyn PhysicalOperator>;

        HashJoin::new(
            left,
            right,
            join_type,
            vec![
                JoinCondition::new(
                    reference_i32(0),
                    reference_i32(0),
                    JoinComparisonType::Equal,
                ),
                JoinCondition::new(
                    reference_i32(0),
                    reference_i32(0),
                    JoinComparisonType::GreaterThan,
                ),
            ],
            vec![],
            vec![],
        )
        .expect("hash join with residual condition should be created")
    }

    fn build_hash_table(
        join_type: JoinType,
        build_keys: &[i32],
        build_payload: &[i32],
    ) -> Arc<JoinHashTable> {
        let ht = Arc::new(JoinHashTable::new(
            create_test_buffer_pool(),
            paro_common::test_utils::test_allocator(),
            vec![equality_condition()],
            vec![LogicalType::Integer],
            join_type,
            JoinHashTableConfig::default(),
        ));

        let keys = Chunk::from_arc_vectors(
            vec![Arc::new(
                paro_common::test_utils::test_i32_vector_with_allocator(
                    build_keys,
                    paro_common::test_utils::test_allocator(),
                ),
            )],
            paro_common::test_utils::test_allocator(),
        );
        let payload = Chunk::from_arc_vectors(
            vec![Arc::new(
                paro_common::test_utils::test_i32_vector_with_allocator(
                    build_payload,
                    paro_common::test_utils::test_allocator(),
                ),
            )],
            paro_common::test_utils::test_allocator(),
        );
        ht.build(&keys, &payload).unwrap();
        ht.finalize().unwrap();
        ht
    }

    fn chunk_from_optional_i32(values: &[Option<i32>]) -> Chunk {
        let mut chunk = paro_common::test_utils::test_chunk_with_capacity(
            &[LogicalType::Integer],
            values.len(),
        );
        for (row_idx, value) in values.iter().enumerate() {
            let column = chunk.column_mut(0).expect("column must exist");
            match value {
                Some(value) => column.set_value(row_idx, &Value::Integer(*value)),
                None => column.set_value(row_idx, &Value::Null(LogicalType::Integer)),
            }
        }
        chunk.set_cardinality(values.len());
        chunk
    }

    fn build_optional_hash_table(
        join_type: JoinType,
        condition: JoinCondition,
        build_keys: &[Option<i32>],
        build_payload: &[Option<i32>],
    ) -> Arc<JoinHashTable> {
        let ht = Arc::new(JoinHashTable::new(
            create_test_buffer_pool(),
            paro_common::test_utils::test_allocator(),
            vec![condition],
            vec![LogicalType::Integer],
            join_type,
            JoinHashTableConfig::default(),
        ));

        let keys = chunk_from_optional_i32(build_keys);
        let payload = chunk_from_optional_i32(build_payload);
        ht.build(&keys, &payload).unwrap();
        ht.finalize().unwrap();
        ht
    }

    fn set_found_flags(ht: &JoinHashTable, flags: &[bool]) {
        let row_ptrs = ht.all_build_row_ptrs();
        assert_eq!(row_ptrs.len(), flags.len());
        for (row_ptr, found) in row_ptrs.into_iter().zip(flags.iter().copied()) {
            ht.set_build_side_found(row_ptr, found);
        }
    }

    #[test]
    fn validate_join_type_accepts_completed_hash_join_shapes() {
        assert!(HashJoin::validate_join_type(JoinType::Inner).is_ok());
        assert!(HashJoin::validate_join_type(JoinType::Left).is_ok());
        assert!(HashJoin::validate_join_type(JoinType::Right).is_ok());
        assert!(HashJoin::validate_join_type(JoinType::Outer).is_ok());
        assert!(HashJoin::validate_join_type(JoinType::Semi).is_ok());
        assert!(HashJoin::validate_join_type(JoinType::Anti).is_ok());
        assert!(HashJoin::validate_join_type(JoinType::Mark).is_ok());
        assert!(HashJoin::validate_join_type(JoinType::Single).is_ok());
        assert!(HashJoin::validate_join_type(JoinType::RightSemi).is_ok());
        assert!(HashJoin::validate_join_type(JoinType::RightAnti).is_ok());
        assert!(HashJoin::validate_join_type(JoinType::Invalid).is_err());
    }

    #[test]
    fn explain_params_always_include_external_flag() {
        let join = create_test_join(JoinType::Inner);

        let params_without_sink = join.explain_params();
        assert!(
            params_without_sink
                .iter()
                .any(|param| param == "External: false"),
            "expected default external flag in explain params, got: {params_without_sink:?}"
        );

        let sink_state = create_test_global_sink_state(
            build_hash_table(JoinType::Inner, &[1, 2], &[10, 20]),
            vec![LogicalType::Integer],
            JoinType::Inner,
        );
        join.set_sink_state(sink_state as Arc<dyn GlobalSinkState>);

        let params_with_sink = join.explain_params();
        assert!(
            params_with_sink
                .iter()
                .any(|param| param == "External: false"),
            "expected runtime external flag in explain params, got: {params_with_sink:?}"
        );
    }

    #[test]
    fn hash_join_finalize_runs_after_sink_finalize() {
        let join = create_test_join(JoinType::Inner);
        let session = create_test_session();
        let thread = ThreadContext::new(0, 1);
        let ctx = ExecutionContext::new(session, &thread, None);

        let gstate_box = join
            .get_global_sink_state(&ctx)
            .expect("global sink state should be created");
        let gstate: Arc<dyn GlobalSinkState> = gstate_box.into();
        join.set_sink_state(gstate.clone());

        let mut lstate = join
            .get_local_sink_state(&ctx)
            .expect("local sink state should be created");
        let interrupt = InterruptState::new();

        let build_chunk = Chunk::from_arc_vectors(
            vec![Arc::new(
                paro_common::test_utils::test_i32_vector_with_allocator(
                    &[1, 2, 3],
                    paro_common::test_utils::test_allocator(),
                ),
            )],
            paro_common::test_utils::test_allocator(),
        );
        let mut sink_input = OperatorSinkInput::new(gstate.as_ref(), lstate.as_mut(), &interrupt);
        join.sink(&ctx, &build_chunk, &mut sink_input)
            .expect("sink should accept build chunk");

        let mut combine_input =
            OperatorSinkCombineInput::new(gstate.as_ref(), lstate.as_mut(), &interrupt);
        join.combine(&ctx, &mut combine_input)
            .expect("combine should succeed");

        let sink_state = gstate
            .as_any()
            .downcast_ref::<HashJoinGlobalSinkState>()
            .expect("sink state should be hash join state");
        assert!(!sink_state.finalize_prepared.load(Ordering::Acquire));
        assert!(!sink_state.finalized.load(Ordering::Acquire));

        join.prepare_finalize(gstate.as_ref())
            .expect("prepare finalize should succeed");
        assert!(sink_state.finalize_prepared.load(Ordering::Acquire));
        assert!(!sink_state.finalized.load(Ordering::Acquire));

        let finalize_result = join
            .finalize(&OperatorSinkFinalizeInput::new(gstate.as_ref(), &interrupt))
            .expect("finalize should succeed");
        assert_eq!(finalize_result, SinkFinalizeType::Ready);
        assert!(sink_state.finalized.load(Ordering::Acquire));

        let finalize_result_again = join
            .finalize(&OperatorSinkFinalizeInput::new(gstate.as_ref(), &interrupt))
            .expect("second finalize should be idempotent");
        assert_eq!(finalize_result_again, SinkFinalizeType::Ready);
    }

    #[test]
    fn force_external_requires_temp_directory() {
        let join = create_column_ref_test_join(JoinType::Inner);
        let session = TestStatementContextBuilder::minimal()
            .with_limits(RuntimeLimits {
                max_threads: 1,
                max_memory: 64 * 1024 * 1024,
                use_temporary_directory: false,
                temporary_directory: String::new(),
                max_temp_directory_size: None,
                force_external: true,
            })
            .build();
        let thread = ThreadContext::new(0, 1);
        let ctx = ExecutionContext::new(session, &thread, None);

        let gstate_box = join
            .get_global_sink_state(&ctx)
            .expect("global sink state should be created");
        let gstate: Arc<dyn GlobalSinkState> = gstate_box.into();
        join.set_sink_state(gstate.clone());

        let mut lstate = join
            .get_local_sink_state(&ctx)
            .expect("local sink state should be created");
        let interrupt = InterruptState::new();
        let build_chunk = Chunk::from_arc_vectors(
            vec![Arc::new(
                paro_common::test_utils::test_i32_vector_with_allocator(
                    &[1, 2],
                    paro_common::test_utils::test_allocator(),
                ),
            )],
            paro_common::test_utils::test_allocator(),
        );
        let mut sink_input = OperatorSinkInput::new(gstate.as_ref(), lstate.as_mut(), &interrupt);
        join.sink(&ctx, &build_chunk, &mut sink_input)
            .expect("sink should accept build chunk");

        let mut combine_input =
            OperatorSinkCombineInput::new(gstate.as_ref(), lstate.as_mut(), &interrupt);
        join.combine(&ctx, &mut combine_input)
            .expect("combine should succeed");

        let err = join
            .finalize(&OperatorSinkFinalizeInput::new(gstate.as_ref(), &interrupt))
            .expect_err("force_external without temp_directory should fail");
        assert!(
            err.to_string()
                .contains("force_external requires a temporary directory"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn mark_join_execute_returns_need_more_input_after_single_batch() {
        let join = create_test_join(JoinType::Mark);
        let hash_table = build_hash_table(JoinType::Mark, &[1], &[10]);
        let sink_state =
            create_test_global_sink_state(hash_table, vec![LogicalType::Integer], JoinType::Mark);
        sink_state.finalized.store(true, Ordering::Release);
        join.set_sink_state(sink_state as Arc<dyn GlobalSinkState>);

        let session = create_test_session();
        let thread = ThreadContext::new(0, 1);
        let ctx = ExecutionContext::new(session, &thread, None);
        let mut state = join.get_operator_state(&ctx).unwrap();
        let input = Chunk::from_arc_vectors(
            vec![Arc::new(
                paro_common::test_utils::test_i32_vector_with_allocator(
                    &[7, 8],
                    paro_common::test_utils::test_allocator(),
                ),
            )],
            paro_common::test_utils::test_allocator(),
        );
        let mut output = Chunk::try_new(paro_common::test_utils::test_allocator())
            .expect("test chunk allocation failed");
        let gstate = EmptyGlobalOperatorState;

        let result = join
            .execute(
                &ctx,
                &input,
                &mut output,
                &gstate,
                state.as_mut(),
                crate::operator::state::test_operator_memory_scope(),
            )
            .unwrap();

        assert_eq!(result, OperatorResultType::NeedMoreInput);
        assert_eq!(output.size(), 2);
        assert_eq!(output.data[1].get_value(0).to_string(), "true");
        assert_eq!(output.data[1].get_value(1).to_string(), "true");
    }

    #[test]
    fn mark_join_with_zero_column_probe_chunk_still_emits_single_boolean_row() {
        let left = Arc::new(PhysicalDummyScan::new()) as Arc<dyn PhysicalOperator>;
        let right = Arc::new(PhysicalDummyScan::with_types(vec![LogicalType::Integer]))
            as Arc<dyn PhysicalOperator>;
        let join = HashJoin::new(
            left,
            right,
            JoinType::Mark,
            vec![JoinCondition::new(
                constant_i32(5),
                reference_i32(0),
                JoinComparisonType::Equal,
            )],
            vec![],
            vec![],
        )
        .expect("hash join should be created");
        let hash_table = Arc::new(JoinHashTable::new(
            create_test_buffer_pool(),
            paro_common::test_utils::test_allocator(),
            vec![JoinCondition::new(
                constant_i32(5),
                reference_i32(0),
                JoinComparisonType::Equal,
            )],
            vec![LogicalType::Integer],
            JoinType::Mark,
            JoinHashTableConfig::default(),
        ));
        let keys = Chunk::from_arc_vectors(
            vec![Arc::new(
                paro_common::test_utils::test_i32_vector_with_allocator(
                    &[5],
                    paro_common::test_utils::test_allocator(),
                ),
            )],
            paro_common::test_utils::test_allocator(),
        );
        let payload = Chunk::from_arc_vectors(
            vec![Arc::new(
                paro_common::test_utils::test_i32_vector_with_allocator(
                    &[5],
                    paro_common::test_utils::test_allocator(),
                ),
            )],
            paro_common::test_utils::test_allocator(),
        );
        hash_table.build(&keys, &payload).unwrap();
        hash_table.finalize().unwrap();

        let sink_state =
            create_test_global_sink_state(hash_table, vec![LogicalType::Integer], JoinType::Mark);
        sink_state.finalized.store(true, Ordering::Release);
        join.set_sink_state(sink_state as Arc<dyn GlobalSinkState>);

        let session = create_test_session();
        let thread = ThreadContext::new(0, 1);
        let ctx = ExecutionContext::new(session, &thread, None);
        let mut state = join.get_operator_state(&ctx).unwrap();
        let mut input = Chunk::try_new(paro_common::test_utils::test_allocator())
            .expect("test chunk allocation failed");
        input.set_cardinality(1);
        let mut output = Chunk::try_new(paro_common::test_utils::test_allocator())
            .expect("test chunk allocation failed");
        let gstate = EmptyGlobalOperatorState;

        let result = join
            .execute(
                &ctx,
                &input,
                &mut output,
                &gstate,
                state.as_mut(),
                crate::operator::state::test_operator_memory_scope(),
            )
            .unwrap();

        assert_eq!(result, OperatorResultType::NeedMoreInput);
        assert_eq!(output.size(), 1);
        assert_eq!(output.data.len(), 1);
        assert_eq!(output.data[0].get_value(0).to_string(), "true");
    }

    #[test]
    fn mark_join_filter_pushdown_no_match_still_emits_false_marker() {
        let left = Arc::new(PhysicalDummyScan::new()) as Arc<dyn PhysicalOperator>;
        let right = Arc::new(PhysicalDummyScan::with_types(vec![LogicalType::Integer]))
            as Arc<dyn PhysicalOperator>;
        let mut join = HashJoin::new(
            left,
            right,
            JoinType::Mark,
            vec![JoinCondition::new(
                constant_i32(7),
                reference_i32(0),
                JoinComparisonType::Equal,
            )],
            vec![],
            vec![],
        )
        .expect("hash join should be created");
        let hash_table = build_hash_table(JoinType::Mark, &[5], &[5]);
        let mut sink_state =
            create_test_global_sink_state(hash_table, vec![LogicalType::Integer], JoinType::Mark);

        let filter_info = crate::operator::join::join_filter_pushdown::JoinFilterPushdownInfo::new(
            vec![0],
            vec![
                crate::operator::join::join_filter_pushdown::JoinFilterPushdownFilter {
                    join_condition_idx: 0,
                    probe_column:
                        crate::operator::join::join_filter_pushdown::JoinFilterPushdownColumn {
                            filter_idx: 0,
                            filter_col_idx: 0,
                        },
                },
            ],
            vec![LogicalType::Integer],
            false,
        );
        let mut filter_lstate = filter_info.get_local_state();
        let build_keys = Chunk::from_arc_vectors(
            vec![Arc::new(
                paro_common::test_utils::test_i32_vector_with_allocator(
                    &[5],
                    paro_common::test_utils::test_allocator(),
                ),
            )],
            paro_common::test_utils::test_allocator(),
        );
        filter_info.sink(&build_keys, &mut filter_lstate);
        let filter_gstate = filter_info.get_global_state();
        filter_info.combine(&filter_gstate, filter_lstate);

        Arc::get_mut(&mut sink_state)
            .expect("sink state should be uniquely owned in test")
            .filter_gstate = Some(filter_gstate);
        sink_state.finalized.store(true, Ordering::Release);
        join.set_filter_pushdown(filter_info);
        join.set_sink_state(sink_state as Arc<dyn GlobalSinkState>);

        let session = create_test_session();
        let thread = ThreadContext::new(0, 1);
        let ctx = ExecutionContext::new(session, &thread, None);
        let mut state = join.get_operator_state(&ctx).unwrap();
        let mut input = Chunk::try_new(paro_common::test_utils::test_allocator())
            .expect("test chunk allocation failed");
        input.set_cardinality(1);
        let mut output = Chunk::try_new(paro_common::test_utils::test_allocator())
            .expect("test chunk allocation failed");
        let gstate = EmptyGlobalOperatorState;

        let result = join
            .execute(
                &ctx,
                &input,
                &mut output,
                &gstate,
                state.as_mut(),
                crate::operator::state::test_operator_memory_scope(),
            )
            .unwrap();

        assert_eq!(result, OperatorResultType::NeedMoreInput);
        assert_eq!(output.size(), 1);
        assert_eq!(output.data[0].get_value(0).to_string(), "false");
    }

    #[test]
    fn nested_mark_joins_preserve_single_probe_row() {
        let session = create_test_session();
        let thread = ThreadContext::new(0, 1);
        let ctx = ExecutionContext::new(session, &thread, None);
        let gstate = EmptyGlobalOperatorState;

        let inner_left = Arc::new(PhysicalDummyScan::new()) as Arc<dyn PhysicalOperator>;
        let inner_right = Arc::new(PhysicalDummyScan::with_types(vec![LogicalType::Integer]))
            as Arc<dyn PhysicalOperator>;
        let inner_join = Arc::new(
            HashJoin::new(
                inner_left,
                inner_right,
                JoinType::Mark,
                vec![JoinCondition::new(
                    constant_i32(5),
                    reference_i32(0),
                    JoinComparisonType::Equal,
                )],
                vec![],
                vec![],
            )
            .expect("hash join should be created"),
        );
        let inner_hash_table = Arc::new(JoinHashTable::new(
            create_test_buffer_pool(),
            paro_common::test_utils::test_allocator(),
            vec![JoinCondition::new(
                constant_i32(5),
                reference_i32(0),
                JoinComparisonType::Equal,
            )],
            vec![LogicalType::Integer],
            JoinType::Mark,
            JoinHashTableConfig::default(),
        ));
        let inner_keys = Chunk::from_arc_vectors(
            vec![Arc::new(
                paro_common::test_utils::test_i32_vector_with_allocator(
                    &[5],
                    paro_common::test_utils::test_allocator(),
                ),
            )],
            paro_common::test_utils::test_allocator(),
        );
        let inner_payload = Chunk::from_arc_vectors(
            vec![Arc::new(
                paro_common::test_utils::test_i32_vector_with_allocator(
                    &[5],
                    paro_common::test_utils::test_allocator(),
                ),
            )],
            paro_common::test_utils::test_allocator(),
        );
        inner_hash_table.build(&inner_keys, &inner_payload).unwrap();
        inner_hash_table.finalize().unwrap();
        let inner_sink = create_test_global_sink_state(
            inner_hash_table,
            vec![LogicalType::Integer],
            JoinType::Mark,
        );
        inner_sink.finalized.store(true, Ordering::Release);
        inner_join.set_sink_state(inner_sink as Arc<dyn GlobalSinkState>);

        let mut inner_state = inner_join.get_operator_state(&ctx).unwrap();
        let mut dummy_input = Chunk::try_new(paro_common::test_utils::test_allocator())
            .expect("test chunk allocation failed");
        dummy_input.set_cardinality(1);
        let mut inner_output = Chunk::try_new(paro_common::test_utils::test_allocator())
            .expect("test chunk allocation failed");
        inner_join
            .execute(
                &ctx,
                &dummy_input,
                &mut inner_output,
                &gstate,
                inner_state.as_mut(),
                crate::operator::state::test_operator_memory_scope(),
            )
            .unwrap();
        assert_eq!(inner_output.len(), 1);
        assert_eq!(inner_output.column_count(), 1);

        let outer_right = Arc::new(PhysicalDummyScan::with_types(vec![LogicalType::Integer]))
            as Arc<dyn PhysicalOperator>;
        let outer_join = HashJoin::new(
            inner_join as Arc<dyn PhysicalOperator>,
            outer_right,
            JoinType::Mark,
            vec![JoinCondition::new(
                constant_i32(7),
                reference_i32(0),
                JoinComparisonType::Equal,
            )],
            vec![],
            vec![],
        )
        .expect("hash join should be created");
        let outer_hash_table = Arc::new(JoinHashTable::new(
            create_test_buffer_pool(),
            paro_common::test_utils::test_allocator(),
            vec![JoinCondition::new(
                constant_i32(7),
                reference_i32(0),
                JoinComparisonType::Equal,
            )],
            vec![LogicalType::Integer],
            JoinType::Mark,
            JoinHashTableConfig::default(),
        ));
        let outer_keys = Chunk::from_arc_vectors(
            vec![Arc::new(
                paro_common::test_utils::test_i32_vector_with_allocator(
                    &[5],
                    paro_common::test_utils::test_allocator(),
                ),
            )],
            paro_common::test_utils::test_allocator(),
        );
        let outer_payload = Chunk::from_arc_vectors(
            vec![Arc::new(
                paro_common::test_utils::test_i32_vector_with_allocator(
                    &[5],
                    paro_common::test_utils::test_allocator(),
                ),
            )],
            paro_common::test_utils::test_allocator(),
        );
        outer_hash_table.build(&outer_keys, &outer_payload).unwrap();
        outer_hash_table.finalize().unwrap();
        let outer_sink = create_test_global_sink_state(
            outer_hash_table,
            vec![LogicalType::Integer],
            JoinType::Mark,
        );
        outer_sink.finalized.store(true, Ordering::Release);
        outer_join.set_sink_state(outer_sink as Arc<dyn GlobalSinkState>);

        let mut outer_state = outer_join.get_operator_state(&ctx).unwrap();
        let mut outer_output = Chunk::try_new(paro_common::test_utils::test_allocator())
            .expect("test chunk allocation failed");
        outer_join
            .execute(
                &ctx,
                &inner_output,
                &mut outer_output,
                &gstate,
                outer_state.as_mut(),
                crate::operator::state::test_operator_memory_scope(),
            )
            .unwrap();

        assert_eq!(outer_output.len(), 1);
        assert_eq!(outer_output.column_count(), 2);
        assert_eq!(outer_output.data[0].get_value(0).to_string(), "true");
        assert_eq!(outer_output.data[1].get_value(0).to_string(), "false");
    }

    #[test]
    fn executor_keeps_rows_for_multiple_uncorrelated_mark_subqueries() {
        let session = create_test_session();

        let first_build_source = Arc::new(PhysicalExpressionScan::new(
            vec![vec![constant_i32(5)]],
            vec![LogicalType::Integer],
        )) as Arc<dyn PhysicalOperator>;
        let first_left = Arc::new(PhysicalDummyScan::new()) as Arc<dyn PhysicalOperator>;
        let first_right = Arc::new(Projection::new(
            vec![Expression::Reference(ReferenceExpression::new(
                0,
                LogicalType::Integer,
            ))],
            first_build_source,
        )) as Arc<dyn PhysicalOperator>;
        let first_join = Arc::new(
            HashJoin::new(
                first_left,
                first_right,
                JoinType::Mark,
                vec![JoinCondition::new(
                    constant_i32(5),
                    reference_i32(0),
                    JoinComparisonType::Equal,
                )],
                vec![],
                vec![],
            )
            .expect("hash join should be created"),
        );

        let second_build_source = Arc::new(PhysicalExpressionScan::new(
            vec![vec![constant_i32(5)]],
            vec![LogicalType::Integer],
        )) as Arc<dyn PhysicalOperator>;
        let second_right = Arc::new(Projection::new(
            vec![Expression::Reference(ReferenceExpression::new(
                0,
                LogicalType::Integer,
            ))],
            second_build_source,
        )) as Arc<dyn PhysicalOperator>;
        let second_join = Arc::new(
            HashJoin::new(
                first_join as Arc<dyn PhysicalOperator>,
                second_right,
                JoinType::Mark,
                vec![JoinCondition::new(
                    constant_i32(7),
                    reference_i32(0),
                    JoinComparisonType::Equal,
                )],
                vec![],
                vec![],
            )
            .expect("hash join should be created"),
        );

        let projection = Arc::new(Projection::new(
            vec![
                Expression::Reference(ReferenceExpression::new(0, LogicalType::Boolean)),
                Expression::Operator(OperatorExpression::new(
                    OperatorType::Not,
                    vec![Expression::Reference(ReferenceExpression::new(
                        1,
                        LogicalType::Boolean,
                    ))],
                    LogicalType::Boolean,
                )),
            ],
            second_join as Arc<dyn PhysicalOperator>,
        )) as Arc<dyn PhysicalOperator>;

        let executor = Executor::new(session);
        let compiled = CompiledStatement {
            physical_plan: projection,
            result_schema: vec![
                ResultColumnDesc::new("in_present", LogicalType::Boolean),
                ResultColumnDesc::new("not_in_absent", LogicalType::Boolean),
            ],
            parameter_types: vec![],
        };
        let mut stream = executor.execute(compiled).unwrap();

        let chunk = stream.fetch().unwrap().expect("expected one result chunk");
        assert_eq!(chunk.len(), 1);
        assert_eq!(chunk.column_count(), 2);
        assert_eq!(chunk.data[0].get_value(0).to_string(), "true");
        assert_eq!(chunk.data[1].get_value(0).to_string(), "true");
        assert!(stream.fetch().unwrap().is_none());
    }

    #[test]
    fn left_join_execute_emits_matches_then_unmatched_without_reprobe() {
        let join = create_column_ref_test_join(JoinType::Left);
        let hash_table = build_hash_table(JoinType::Left, &[2, 4], &[20, 40]);
        let sink_state =
            create_test_global_sink_state(hash_table, vec![LogicalType::Integer], JoinType::Left);
        sink_state.finalized.store(true, Ordering::Release);
        join.set_sink_state(sink_state as Arc<dyn GlobalSinkState>);

        let session = create_test_session();
        let thread = ThreadContext::new(0, 1);
        let ctx = ExecutionContext::new(session, &thread, None);
        let mut state = join.get_operator_state(&ctx).unwrap();
        let input = Chunk::from_arc_vectors(
            vec![Arc::new(
                paro_common::test_utils::test_i32_vector_with_allocator(
                    &[1, 2, 4],
                    paro_common::test_utils::test_allocator(),
                ),
            )],
            paro_common::test_utils::test_allocator(),
        );
        let gstate = EmptyGlobalOperatorState;

        let mut first_output = paro_common::test_utils::test_chunk_with_capacity(
            &[LogicalType::BigInt],
            paro_common::vector::VECTOR_SIZE,
        );
        let first_result = join
            .execute(
                &ctx,
                &input,
                &mut first_output,
                &gstate,
                state.as_mut(),
                crate::operator::state::test_operator_memory_scope(),
            )
            .unwrap();
        assert_eq!(first_result, OperatorResultType::HaveMoreOutput);
        assert_eq!(first_output.size(), 2);
        assert_eq!(first_output.data[0].get_value(0).to_string(), "2");
        assert_eq!(first_output.data[1].get_value(0).to_string(), "20");
        assert_eq!(first_output.data[0].get_value(1).to_string(), "4");
        assert_eq!(first_output.data[1].get_value(1).to_string(), "40");

        let mut second_output = first_output.clone();
        let second_result = join
            .execute(
                &ctx,
                &input,
                &mut second_output,
                &gstate,
                state.as_mut(),
                crate::operator::state::test_operator_memory_scope(),
            )
            .unwrap();
        assert_eq!(second_result, OperatorResultType::NeedMoreInput);
        assert_eq!(second_output.size(), 1);
        assert_eq!(second_output.data[0].get_value(0).to_string(), "1");
        assert!(second_output.data[1].is_null(0));
    }

    #[test]
    fn right_join_source_emits_unmatched_build_rows() {
        let join = create_test_join(JoinType::Right);
        let hash_table = build_hash_table(JoinType::Right, &[1, 2], &[10, 20]);
        set_found_flags(&hash_table, &[true, false]);

        let session = create_test_session();
        let thread = ThreadContext::new(0, 1);
        let ctx = ExecutionContext::new(session, &thread, None);
        let gstate = HashJoinGlobalSourceState {
            hash_table,
            join_type: JoinType::Right,
        };
        let mut lstate = join.get_local_source_state(&ctx, &gstate).unwrap();
        let interrupt = InterruptState::new();
        let mut input = OperatorSourceInput::new(&gstate, lstate.as_mut(), &interrupt);
        let mut output = Chunk::try_new(paro_common::test_utils::test_allocator())
            .expect("test chunk allocation failed");

        let result = join.get_data(&ctx, &mut output, &mut input).unwrap();

        assert_eq!(result, SourceResultType::HaveMoreOutput);
        assert_eq!(output.size(), 1);
        assert!(output.data[0].is_null(0));
        assert_eq!(output.data[1].get_value(0).to_string(), "20");
        assert_eq!(
            join.get_data(&ctx, &mut output, &mut input).unwrap(),
            SourceResultType::Finished
        );
    }

    #[test]
    fn force_external_right_join_scans_build_side_without_probe_input() {
        let join = create_column_ref_test_join(JoinType::Right);
        let (session, temp_dir) = create_force_external_session(64 * 1024 * 1024);
        let thread = ThreadContext::new(0, 1);
        let ctx = ExecutionContext::new(session, &thread, None);

        let gstate_box = join
            .get_global_sink_state(&ctx)
            .expect("global sink state should be created");
        let gstate: Arc<dyn GlobalSinkState> = gstate_box.into();
        join.set_sink_state(gstate.clone());

        let mut lstate = join
            .get_local_sink_state(&ctx)
            .expect("local sink state should be created");
        let interrupt = InterruptState::new();
        let build_chunk = Chunk::from_arc_vectors(
            vec![Arc::new(
                paro_common::test_utils::test_i32_vector_with_allocator(
                    &[1, 2],
                    paro_common::test_utils::test_allocator(),
                ),
            )],
            paro_common::test_utils::test_allocator(),
        );
        let mut sink_input = OperatorSinkInput::new(gstate.as_ref(), lstate.as_mut(), &interrupt);
        join.sink(&ctx, &build_chunk, &mut sink_input)
            .expect("sink should accept build chunk");

        let mut combine_input =
            OperatorSinkCombineInput::new(gstate.as_ref(), lstate.as_mut(), &interrupt);
        join.combine(&ctx, &mut combine_input)
            .expect("combine should succeed");

        let finalize_result = join
            .finalize(&OperatorSinkFinalizeInput::new(gstate.as_ref(), &interrupt))
            .expect("force_external right join should finalize");
        assert_eq!(finalize_result, SinkFinalizeType::Ready);

        let sink_state = gstate
            .as_any()
            .downcast_ref::<HashJoinGlobalSinkState>()
            .expect("sink state should be hash join state");
        assert!(sink_state.externalized());
        assert!(sink_state
            .external_fallback_reason
            .lock()
            .unwrap()
            .is_none());

        let mut operator_state = join
            .get_operator_state(&ctx)
            .expect("operator state should be created");
        let goperator = EmptyGlobalOperatorState;

        loop {
            let mut final_output = Chunk::try_new(paro_common::test_utils::test_allocator())
                .expect("test chunk allocation failed");
            let result = join
                .final_execute(
                    &ctx,
                    &mut final_output,
                    &goperator,
                    operator_state.as_mut(),
                    crate::operator::state::test_operator_memory_scope(),
                )
                .expect("final execute should succeed");
            if result == OperatorFinalizeResultType::Finished {
                break;
            }
        }

        let gsource = join
            .get_global_source_state(&ctx, Some(gstate.as_ref()))
            .expect("global source state should be created");
        let mut lsource = join
            .get_local_source_state(&ctx, gsource.as_ref())
            .expect("local source state should be created");
        let mut source_input =
            OperatorSourceInput::new(gsource.as_ref(), lsource.as_mut(), &interrupt);
        let mut seen_rows = Vec::new();
        let mut source_output = paro_common::test_utils::test_chunk_with_capacity(
            &[LogicalType::BigInt],
            paro_common::vector::VECTOR_SIZE,
        );
        loop {
            let source_result = join
                .get_data(&ctx, &mut source_output, &mut source_input)
                .expect("source get_data should succeed");
            for row_idx in 0..source_output.size() {
                seen_rows.push((
                    source_output.data[0].is_null(row_idx),
                    source_output.data[1].get_i32(row_idx),
                ));
            }
            if source_result == SourceResultType::Finished {
                break;
            }
        }
        assert_eq!(seen_rows.len(), 2);
        assert_eq!(seen_rows[0], (true, Some(1)));
        assert_eq!(seen_rows[1], (true, Some(2)));

        let _ = fs::remove_dir_all(temp_dir);
    }

    #[test]
    fn force_external_non_source_without_source_rows_finishes_without_deadlock() {
        let join = create_column_ref_test_join(JoinType::Inner);
        let (session, temp_dir) = create_force_external_session(64 * 1024 * 1024);
        let thread = ThreadContext::new(0, 1);
        let ctx = ExecutionContext::new(session, &thread, None);

        let gstate_box = join
            .get_global_sink_state(&ctx)
            .expect("global sink state should be created");
        let gstate: Arc<dyn GlobalSinkState> = gstate_box.into();
        join.set_sink_state(gstate.clone());

        let sink_state = gstate
            .as_any()
            .downcast_ref::<HashJoinGlobalSinkState>()
            .expect("sink state should be hash join state");
        sink_state.externalized.store(true, Ordering::Release);
        *sink_state.external_source_rows.lock().unwrap() = None;

        let gsource = join
            .get_global_source_state(&ctx, Some(gstate.as_ref()))
            .expect("global source state should be created");
        let mut lsource = join
            .get_local_source_state(&ctx, gsource.as_ref())
            .expect("local source state should be created");
        let interrupt = InterruptState::new();
        let mut source_input =
            OperatorSourceInput::new(gsource.as_ref(), lsource.as_mut(), &interrupt);
        let mut output = Chunk::try_new(paro_common::test_utils::test_allocator())
            .expect("test chunk allocation failed");

        let result = join
            .get_data(&ctx, &mut output, &mut source_input)
            .expect("source get_data should finish cleanly");

        assert_eq!(result, SourceResultType::Finished);
        assert_eq!(output.size(), 0);
        assert!(
            sink_state.external_source_rows.try_lock().is_ok(),
            "source-row cleanup must not leave the mutex locked"
        );

        let _ = fs::remove_dir_all(temp_dir);
    }

    #[test]
    fn force_external_left_join_releases_spill_files_after_finish() {
        let _guard = spill_file_test_guard();
        let join = create_column_ref_test_join(JoinType::Left);
        let (session, temp_dir) = create_force_external_session(32 * 1024 * 1024);
        let thread = ThreadContext::new(0, 1);
        let ctx = ExecutionContext::new(session, &thread, None);
        let pool = ctx.buffer_pool().clone();

        let gstate_box = join
            .get_global_sink_state(&ctx)
            .expect("global sink state should be created");
        let gstate: Arc<dyn GlobalSinkState> = gstate_box.into();
        join.set_sink_state(gstate.clone());

        let mut lstate = join
            .get_local_sink_state(&ctx)
            .expect("local sink state should be created");
        let interrupt = InterruptState::new();

        for start in (1..=20_000_i32).step_by(paro_common::vector::VECTOR_SIZE) {
            let end = (start + paro_common::vector::VECTOR_SIZE as i32 - 1).min(20_000);
            let values: Vec<i32> = (start..=end).collect();
            let build_chunk = Chunk::from_arc_vectors(
                vec![Arc::new(
                    paro_common::test_utils::test_i32_vector_with_allocator(
                        &values,
                        paro_common::test_utils::test_allocator(),
                    ),
                )],
                paro_common::test_utils::test_allocator(),
            );
            let mut sink_input =
                OperatorSinkInput::new(gstate.as_ref(), lstate.as_mut(), &interrupt);
            join.sink(&ctx, &build_chunk, &mut sink_input)
                .expect("sink should accept build chunk");
        }

        let mut combine_input =
            OperatorSinkCombineInput::new(gstate.as_ref(), lstate.as_mut(), &interrupt);
        join.combine(&ctx, &mut combine_input)
            .expect("combine should succeed");

        let finalize_result = join
            .finalize(&OperatorSinkFinalizeInput::new(gstate.as_ref(), &interrupt))
            .expect("force_external left join should finalize");
        assert_eq!(finalize_result, SinkFinalizeType::Ready);

        let sink_state = gstate
            .as_any()
            .downcast_ref::<HashJoinGlobalSinkState>()
            .expect("sink state should be hash join state");
        assert!(sink_state.externalized());

        let mut state = join
            .get_operator_state(&ctx)
            .expect("operator state should be created");
        let goperator = EmptyGlobalOperatorState;

        for start in (1..=20_000_i32).step_by(paro_common::vector::VECTOR_SIZE) {
            let end = (start + paro_common::vector::VECTOR_SIZE as i32 - 1).min(20_000);
            let values: Vec<i32> = (start..=end).collect();
            let input = Chunk::from_arc_vectors(
                vec![Arc::new(
                    paro_common::test_utils::test_i32_vector_with_allocator(
                        &values,
                        paro_common::test_utils::test_allocator(),
                    ),
                )],
                paro_common::test_utils::test_allocator(),
            );
            let mut output = Chunk::try_new(paro_common::test_utils::test_allocator())
                .expect("test chunk allocation failed");
            let result = join
                .execute(
                    &ctx,
                    &input,
                    &mut output,
                    &goperator,
                    state.as_mut(),
                    crate::operator::state::test_operator_memory_scope(),
                )
                .expect("execute should succeed");
            assert!(
                matches!(
                    result,
                    OperatorResultType::NeedMoreInput | OperatorResultType::HaveMoreOutput
                ),
                "unexpected execute result: {result:?}"
            );
        }

        loop {
            let mut final_output = Chunk::try_new(paro_common::test_utils::test_allocator())
                .expect("test chunk allocation failed");
            let result = join
                .final_execute(
                    &ctx,
                    &mut final_output,
                    &goperator,
                    state.as_mut(),
                    crate::operator::state::test_operator_memory_scope(),
                )
                .expect("final execute should succeed");
            if result == OperatorFinalizeResultType::Finished {
                break;
            }
        }

        assert!(
            pool.get_temporary_files().is_empty(),
            "temporary spill files should be released after hash join completion"
        );
        pool.set_temporary_directory(String::new())
            .expect("temp directory should reset after spill cleanup");

        let _ = fs::remove_dir_all(temp_dir);
    }

    #[test]
    fn force_external_finalize_cleanup_releases_spill_files_under_tight_budget() {
        let _guard = spill_file_test_guard();
        let join = create_column_ref_test_join(JoinType::Left);
        let (session, temp_dir) = create_force_external_session(2 * 1024 * 1024);
        let thread = ThreadContext::new(0, 1);
        let ctx = ExecutionContext::new(session, &thread, None);
        let pool = ctx.buffer_pool().clone();

        let gstate_box = join
            .get_global_sink_state(&ctx)
            .expect("global sink state should be created");
        let gstate: Arc<dyn GlobalSinkState> = gstate_box.into();
        join.set_sink_state(gstate.clone());

        let mut lstate = join
            .get_local_sink_state(&ctx)
            .expect("local sink state should be created");
        let interrupt = InterruptState::new();

        for start in (1..=20_000_i32).step_by(paro_common::vector::VECTOR_SIZE) {
            let end = (start + paro_common::vector::VECTOR_SIZE as i32 - 1).min(20_000);
            let values: Vec<i32> = (start..=end).collect();
            let build_chunk = Chunk::from_arc_vectors(
                vec![Arc::new(
                    paro_common::test_utils::test_i32_vector_with_allocator(
                        &values,
                        paro_common::test_utils::test_allocator(),
                    ),
                )],
                paro_common::test_utils::test_allocator(),
            );
            let mut sink_input =
                OperatorSinkInput::new(gstate.as_ref(), lstate.as_mut(), &interrupt);
            join.sink(&ctx, &build_chunk, &mut sink_input)
                .expect("sink should accept build chunk");
        }

        let mut combine_input =
            OperatorSinkCombineInput::new(gstate.as_ref(), lstate.as_mut(), &interrupt);
        join.combine(&ctx, &mut combine_input)
            .expect("combine should succeed");

        let finalize_result = join
            .finalize(&OperatorSinkFinalizeInput::new(gstate.as_ref(), &interrupt))
            .expect("finalize should succeed under the constrained memory budget");
        assert_eq!(finalize_result, SinkFinalizeType::Ready);

        let sink_state = gstate
            .as_any()
            .downcast_ref::<HashJoinGlobalSinkState>()
            .expect("sink state should be hash join state");
        assert!(sink_state.externalized());
        sink_state.cleanup_external_spill_state(true);

        assert!(
            pool.get_temporary_files().is_empty(),
            "temporary spill files should be released after tight-budget cleanup"
        );
        pool.set_temporary_directory(String::new())
            .expect("temp directory should reset after tight-budget cleanup");

        let _ = fs::remove_dir_all(temp_dir);
    }

    #[test]
    fn force_external_mark_join_buffers_correctly_under_order_by() {
        let (session, temp_dir) = create_force_external_session(64 * 1024 * 1024);

        let build_source = Arc::new(PhysicalExpressionScan::new(
            vec![
                vec![constant_i32(1)],
                vec![constant_i32(1)],
                vec![constant_i32(2)],
                vec![constant_i32(2)],
                vec![constant_i32(3)],
                vec![Expression::Constant(ConstantExpression::new(
                    Value::Null(LogicalType::Integer),
                    LogicalType::Integer,
                ))],
                vec![constant_i32(5)],
            ],
            vec![LogicalType::Integer],
        )) as Arc<dyn PhysicalOperator>;
        let left_source = Arc::new(PhysicalExpressionScan::new(
            vec![
                vec![constant_i32(1), constant_i32(1)],
                vec![constant_i32(2), constant_i32(1)],
                vec![constant_i32(3), constant_i32(2)],
                vec![constant_i32(4), constant_i32(3)],
                vec![
                    constant_i32(5),
                    Expression::Constant(ConstantExpression::new(
                        Value::Null(LogicalType::Integer),
                        LogicalType::Integer,
                    )),
                ],
                vec![constant_i32(6), constant_i32(4)],
            ],
            vec![LogicalType::Integer, LogicalType::Integer],
        )) as Arc<dyn PhysicalOperator>;
        let right_source = Arc::new(Projection::new(
            vec![Expression::Reference(ReferenceExpression::new(
                0,
                LogicalType::Integer,
            ))],
            build_source,
        )) as Arc<dyn PhysicalOperator>;
        let join = Arc::new(
            HashJoin::new(
                left_source,
                right_source,
                JoinType::Mark,
                vec![JoinCondition::new(
                    reference_i32(1),
                    reference_i32(0),
                    JoinComparisonType::Equal,
                )],
                vec![],
                vec![],
            )
            .expect("hash join should be created"),
        ) as Arc<dyn PhysicalOperator>;
        let projection = Arc::new(Projection::new(
            vec![
                Expression::Reference(ReferenceExpression::new(0, LogicalType::Integer)),
                Expression::Reference(ReferenceExpression::new(2, LogicalType::Boolean)),
            ],
            join,
        )) as Arc<dyn PhysicalOperator>;

        let executor = Executor::new(session.clone());
        let compiled = CompiledStatement {
            physical_plan: projection,
            result_schema: vec![
                ResultColumnDesc::new("id", LogicalType::Integer),
                ResultColumnDesc::new("in_rhs", LogicalType::Boolean),
            ],
            parameter_types: vec![],
        };
        let mut stream = executor.execute(compiled).unwrap();

        let mut rows = Vec::new();
        while let Some(chunk) = stream.fetch().unwrap() {
            for row_idx in 0..chunk.size() {
                rows.push((
                    chunk.data[0].get_i32(row_idx),
                    if chunk.data[1].is_null(row_idx) {
                        None
                    } else {
                        chunk.data[1].get_bool(row_idx)
                    },
                ));
            }
        }

        rows.sort_by_key(|(id, _)| *id);
        assert_eq!(
            rows,
            vec![
                (Some(1), Some(true)),
                (Some(2), Some(true)),
                (Some(3), Some(true)),
                (Some(4), Some(true)),
                (Some(5), None),
                (Some(6), None),
            ]
        );

        let _ = fs::remove_dir_all(temp_dir);
    }

    #[test]
    fn force_external_single_join_returns_one_match_per_probe_row() {
        let join = create_column_ref_test_join(JoinType::Single);
        let (session, temp_dir) = create_force_external_session(64 * 1024 * 1024);
        let thread = ThreadContext::new(0, 1);
        let ctx = ExecutionContext::new(session, &thread, None);

        let gstate_box = join
            .get_global_sink_state(&ctx)
            .expect("global sink state should be created");
        let gstate: Arc<dyn GlobalSinkState> = gstate_box.into();
        join.set_sink_state(gstate.clone());

        let mut lstate = join
            .get_local_sink_state(&ctx)
            .expect("local sink state should be created");
        let interrupt = InterruptState::new();
        let build_chunk = Chunk::from_arc_vectors(
            vec![Arc::new(
                paro_common::test_utils::test_i32_vector_with_allocator(
                    &[1, 2, 4],
                    paro_common::test_utils::test_allocator(),
                ),
            )],
            paro_common::test_utils::test_allocator(),
        );
        let mut sink_input = OperatorSinkInput::new(gstate.as_ref(), lstate.as_mut(), &interrupt);
        join.sink(&ctx, &build_chunk, &mut sink_input)
            .expect("sink should accept build chunk");

        let mut combine_input =
            OperatorSinkCombineInput::new(gstate.as_ref(), lstate.as_mut(), &interrupt);
        join.combine(&ctx, &mut combine_input)
            .expect("combine should succeed");

        let finalize_result = join
            .finalize(&OperatorSinkFinalizeInput::new(gstate.as_ref(), &interrupt))
            .expect("force_external single join should finalize");
        assert_eq!(finalize_result, SinkFinalizeType::Ready);

        let sink_state = gstate
            .as_any()
            .downcast_ref::<HashJoinGlobalSinkState>()
            .expect("sink state should be hash join state");
        assert!(sink_state.externalized());

        let mut state = join.get_operator_state(&ctx).unwrap();
        let input = Chunk::from_arc_vectors(
            vec![Arc::new(
                paro_common::test_utils::test_i32_vector_with_allocator(
                    &[1, 2, 3, 4],
                    paro_common::test_utils::test_allocator(),
                ),
            )],
            paro_common::test_utils::test_allocator(),
        );
        let goperator = EmptyGlobalOperatorState;
        let mut rows = Vec::new();

        let mut output = Chunk::try_new(paro_common::test_utils::test_allocator())
            .expect("test chunk allocation failed");
        let result = join
            .execute(
                &ctx,
                &input,
                &mut output,
                &goperator,
                state.as_mut(),
                crate::operator::state::test_operator_memory_scope(),
            )
            .expect("execute should succeed");
        for row_idx in 0..output.size() {
            rows.push((
                output.data[0].get_i32(row_idx),
                output.data[1].get_i32(row_idx),
            ));
        }
        assert!(
            matches!(
                result,
                OperatorResultType::NeedMoreInput | OperatorResultType::HaveMoreOutput
            ),
            "unexpected execute result: {result:?}"
        );

        loop {
            let mut final_output = Chunk::try_new(paro_common::test_utils::test_allocator())
                .expect("test chunk allocation failed");
            let finalize_result = join
                .final_execute(
                    &ctx,
                    &mut final_output,
                    &goperator,
                    state.as_mut(),
                    crate::operator::state::test_operator_memory_scope(),
                )
                .expect("final execute should succeed");
            for row_idx in 0..final_output.size() {
                rows.push((
                    final_output.data[0].get_i32(row_idx),
                    final_output.data[1].get_i32(row_idx),
                ));
            }
            if finalize_result == OperatorFinalizeResultType::Finished {
                break;
            }
        }

        rows.sort_by_key(|(left, _)| *left);
        assert_eq!(
            rows,
            vec![
                (Some(1), Some(1)),
                (Some(2), Some(2)),
                (Some(3), None),
                (Some(4), Some(4)),
            ]
        );

        let _ = fs::remove_dir_all(temp_dir);
    }

    #[test]
    fn right_semi_source_projects_only_matched_build_rows() {
        let join = create_test_join(JoinType::RightSemi);
        let hash_table = build_hash_table(JoinType::RightSemi, &[1, 2], &[10, 20]);
        set_found_flags(&hash_table, &[true, false]);

        let session = create_test_session();
        let thread = ThreadContext::new(0, 1);
        let ctx = ExecutionContext::new(session, &thread, None);
        let gstate = HashJoinGlobalSourceState {
            hash_table,
            join_type: JoinType::RightSemi,
        };
        let mut lstate = join.get_local_source_state(&ctx, &gstate).unwrap();
        let interrupt = InterruptState::new();
        let mut input = OperatorSourceInput::new(&gstate, lstate.as_mut(), &interrupt);
        let mut output = Chunk::try_new(paro_common::test_utils::test_allocator())
            .expect("test chunk allocation failed");

        let result = join.get_data(&ctx, &mut output, &mut input).unwrap();

        assert_eq!(result, SourceResultType::HaveMoreOutput);
        assert_eq!(output.size(), 1);
        assert_eq!(output.column_count(), 1);
        assert_eq!(output.data[0].get_value(0).to_string(), "10");
    }

    #[test]
    fn right_semi_execute_marks_duplicate_build_rows_before_source_scan() {
        let join = create_column_ref_test_join(JoinType::RightSemi);
        let hash_table =
            build_hash_table(JoinType::RightSemi, &[1, 2, 3, 3, 6], &[10, 20, 30, 31, 60]);
        let sink_state = create_test_global_sink_state(
            hash_table.clone(),
            vec![LogicalType::Integer],
            JoinType::RightSemi,
        );
        sink_state.finalized.store(true, Ordering::Release);
        join.set_sink_state(sink_state as Arc<dyn GlobalSinkState>);

        let session = create_test_session();
        let thread = ThreadContext::new(0, 1);
        let ctx = ExecutionContext::new(session, &thread, None);
        let mut state = join.get_operator_state(&ctx).unwrap();
        let input = Chunk::from_arc_vectors(
            vec![Arc::new(
                paro_common::test_utils::test_i32_vector_with_allocator(
                    &[1, 2, 3, 3, 4],
                    paro_common::test_utils::test_allocator(),
                ),
            )],
            paro_common::test_utils::test_allocator(),
        );
        let gstate = EmptyGlobalOperatorState;

        let mut probe_output = Chunk::try_new(paro_common::test_utils::test_allocator())
            .expect("test chunk allocation failed");
        let probe_result = join
            .execute(
                &ctx,
                &input,
                &mut probe_output,
                &gstate,
                state.as_mut(),
                crate::operator::state::test_operator_memory_scope(),
            )
            .unwrap();
        assert_eq!(probe_result, OperatorResultType::NeedMoreInput);
        assert_eq!(probe_output.size(), 0);

        let source_gstate = HashJoinGlobalSourceState {
            hash_table,
            join_type: JoinType::RightSemi,
        };
        let mut source_lstate = join.get_local_source_state(&ctx, &source_gstate).unwrap();
        let interrupt = InterruptState::new();
        let mut source_input =
            OperatorSourceInput::new(&source_gstate, source_lstate.as_mut(), &interrupt);
        let mut source_output = Chunk::try_new(paro_common::test_utils::test_allocator())
            .expect("test chunk allocation failed");

        let source_result = join
            .get_data(&ctx, &mut source_output, &mut source_input)
            .unwrap();
        assert_eq!(source_result, SourceResultType::HaveMoreOutput);
        assert_eq!(source_output.size(), 4);
        assert_eq!(source_output.data[0].get_value(0).to_string(), "10");
        assert_eq!(source_output.data[0].get_value(1).to_string(), "20");
        assert_eq!(source_output.data[0].get_value(2).to_string(), "30");
        assert_eq!(source_output.data[0].get_value(3).to_string(), "31");
    }

    #[test]
    fn semi_and_anti_execute_keep_null_matches_for_not_distinct_from() {
        let session = create_test_session();
        let thread = ThreadContext::new(0, 1);
        let ctx = ExecutionContext::new(session, &thread, None);
        let gstate = EmptyGlobalOperatorState;

        let semi_join = create_not_distinct_reference_test_join(JoinType::Semi);
        let semi_hash_table = build_optional_hash_table(
            JoinType::Semi,
            not_distinct_reference_condition(),
            &[Some(2), None],
            &[Some(20), None],
        );
        let semi_sink_state = create_test_global_sink_state(
            semi_hash_table,
            vec![LogicalType::Integer],
            JoinType::Semi,
        );
        semi_sink_state.finalized.store(true, Ordering::Release);
        semi_join.set_sink_state(semi_sink_state as Arc<dyn GlobalSinkState>);

        let mut semi_state = semi_join.get_operator_state(&ctx).unwrap();
        let semi_input = chunk_from_optional_i32(&[None, Some(2)]);
        let mut semi_output = Chunk::try_new(paro_common::test_utils::test_allocator())
            .expect("test chunk allocation failed");
        let semi_result = semi_join
            .execute(
                &ctx,
                &semi_input,
                &mut semi_output,
                &gstate,
                semi_state.as_mut(),
                crate::operator::state::test_operator_memory_scope(),
            )
            .unwrap();
        assert_eq!(semi_result, OperatorResultType::NeedMoreInput);
        assert_eq!(semi_output.size(), 2);
        assert!(semi_output.data[0].is_null(0));
        assert_eq!(semi_output.data[0].get_value(1).to_string(), "2");

        let anti_join = create_not_distinct_reference_test_join(JoinType::Anti);
        let anti_hash_table = build_optional_hash_table(
            JoinType::Anti,
            not_distinct_reference_condition(),
            &[Some(2), None],
            &[Some(20), None],
        );
        let anti_sink_state = create_test_global_sink_state(
            anti_hash_table,
            vec![LogicalType::Integer],
            JoinType::Anti,
        );
        anti_sink_state.finalized.store(true, Ordering::Release);
        anti_join.set_sink_state(anti_sink_state as Arc<dyn GlobalSinkState>);

        let mut anti_state = anti_join.get_operator_state(&ctx).unwrap();
        let mut anti_output = Chunk::try_new(paro_common::test_utils::test_allocator())
            .expect("test chunk allocation failed");
        let anti_result = anti_join
            .execute(
                &ctx,
                &semi_input,
                &mut anti_output,
                &gstate,
                anti_state.as_mut(),
                crate::operator::state::test_operator_memory_scope(),
            )
            .unwrap();
        assert_eq!(anti_result, OperatorResultType::NeedMoreInput);
        assert_eq!(anti_output.size(), 0);
    }

    #[test]
    fn source_join_pipeline_uses_child_pipeline_dependency() {
        let join = Arc::new(create_test_join(JoinType::Right)) as Arc<dyn PhysicalOperator>;
        let meta_pipeline = MetaPipeline::new(None, MetaPipelineType::Regular);
        let current = meta_pipeline.base_pipeline();
        let mut state = PipelineBuildState::new();

        join.build_pipelines(&join, &current, &meta_pipeline, &mut state);

        let pipelines = meta_pipeline.pipelines();
        let deps = meta_pipeline.explicit_dependencies();

        assert_eq!(pipelines.len(), 2);
        assert_eq!(deps.len(), 1);
        assert!(Arc::ptr_eq(&deps[0].0, &pipelines[1]));
        assert_eq!(deps[0].1.len(), 1);
        assert!(Arc::ptr_eq(&deps[0].1[0], &pipelines[0]));
    }

    #[test]
    fn operator_state_initializes_cached_residual_condition_executors() {
        let session = create_test_session();
        let thread = ThreadContext::single_threaded();
        let ctx = ExecutionContext::new(session, &thread, None);
        let join = create_residual_reference_test_join(JoinType::Inner);
        let sink = join
            .get_global_sink_state(&ctx)
            .expect("global sink state should be created");
        join.set_sink_state(Arc::from(sink));

        let state = join
            .get_operator_state(&ctx)
            .expect("hash join operator state should be created");
        let state = state
            .as_any()
            .downcast_ref::<super::HashJoinOperatorState>()
            .expect("hash join operator state downcast should succeed");

        assert_eq!(state.residual_condition_executors.len(), 1);
        assert_eq!(
            state.residual_condition_executors[0]
                .left
                .expression_count(),
            1
        );
        assert_eq!(
            state.residual_condition_executors[0]
                .right
                .expression_count(),
            1
        );
    }

    #[test]
    fn hash_join_states_cache_key_executors() {
        let session = create_test_session();
        let thread = ThreadContext::single_threaded();
        let ctx = ExecutionContext::new(session, &thread, None);
        let join = create_column_ref_test_join(JoinType::Inner);
        let sink = join
            .get_global_sink_state(&ctx)
            .expect("global sink state should be created");
        join.set_sink_state(Arc::from(sink));

        let operator_state = join
            .get_operator_state(&ctx)
            .expect("hash join operator state should be created");
        let operator_state = operator_state
            .as_any()
            .downcast_ref::<super::HashJoinOperatorState>()
            .expect("hash join operator state downcast should succeed");
        assert_eq!(operator_state.probe_key_executors.executors.len(), 1);
        assert_eq!(
            operator_state.probe_key_executors.executors[0].expression_count(),
            1
        );

        let local_sink_state = join
            .get_local_sink_state(&ctx)
            .expect("hash join local sink state should be created");
        let local_sink_state = local_sink_state
            .as_any()
            .downcast_ref::<super::HashJoinLocalSinkState>()
            .expect("hash join local sink state downcast should succeed");
        assert_eq!(local_sink_state.build_key_executors.len(), 1);
        assert_eq!(
            local_sink_state.build_key_executors[0].expression_count(),
            1
        );
    }
}
