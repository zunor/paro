use std::hash::{Hash, Hasher};
use std::sync::Arc;

use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::types::LogicalType;
use paro_common::vector::{SelectionVector, Vector};
use paro_planner::operator::join::JoinType;
use paro_storage::buffer::{MemoryTag, DEFAULT_BLOCK_ALLOC_SIZE};
use paro_storage::row::{
    RadixPartitionedRows, RadixPartitionedRowsBuilder, RadixPartitioning, RowScanState, RowStore,
    RowStoreBuilder,
};

use crate::execution_context::ExecutionContext;
use crate::join_hashtable::join_hashtable::JoinHashTable;
use crate::operator::join::join_result_helpers::{
    construct_right_outer_scan_result, construct_semi_join_result,
};
use crate::result_type::{OperatorFinalizeResultType, OperatorResultType, SourceResultType};
use crate::spill::probe_spill::ProbeSpill;

use super::operator::{
    HashJoin, HashJoinGlobalSinkState, HashJoinLocalSourceState, HashJoinOperatorState,
};
use super::probe_engine;

const SKEW_SINGLE_THREADED_THRESHOLD: f64 = 0.33;
const EXTERNAL_BUILD_HEADROOM_BYTES: usize = 16 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum HashJoinExternalProbeStage {
    PrepareBuild,
    Probe,
    ScanBuild,
    Done,
}

#[derive(Debug)]
pub(super) struct HashJoinExternalRuntime {
    pub(super) radix_bits: usize,
    pub(super) sink_collection: RadixPartitionedRows,
    pub(super) completed_partitions: Vec<bool>,
    pub(super) current_partitions: Vec<bool>,
    pub(super) probe_spill: ProbeSpill,
    pub(super) probe_spill_finalized: bool,
    pub(super) probe_rows: Option<RowStore>,
    pub(super) probe_stage: HashJoinExternalProbeStage,
    pub(super) global_has_null: bool,
    pub(super) probe_side_requirement: usize,
    pub(super) source_output_rows_builder: Option<RowStoreBuilder>,
}

impl HashJoinExternalRuntime {
    pub(super) fn partition_count(&self) -> usize {
        self.completed_partitions.len()
    }
}

pub(super) fn row_width_and_constness(types: &[LogicalType]) -> (usize, bool) {
    let mut row_width = 0usize;
    let mut all_constant = true;
    for ty in types {
        row_width = row_width.saturating_add(ty.type_size());
        all_constant &= !matches!(
            ty,
            LogicalType::Varchar
                | LogicalType::TsVector
                | LogicalType::TsQuery
                | LogicalType::List(_)
                | LogicalType::Struct(_)
        );
    }
    let validity_bytes = (types.len().saturating_add(7)) / 8;
    let hash_bytes = std::mem::size_of::<u64>();
    (
        row_width
            .saturating_add(validity_bytes)
            .saturating_add(hash_bytes)
            .max(1),
        all_constant,
    )
}

pub(super) fn get_partitioning_space_requirement(
    probe_types: &[LogicalType],
    radix_bits: usize,
    num_threads: usize,
) -> usize {
    let (row_width, all_constant) = row_width_and_constness(probe_types);
    let rows_per_block = (DEFAULT_BLOCK_ALLOC_SIZE / row_width).max(1);
    let mut blocks_per_chunk =
        (paro_common::vector::VECTOR_SIZE + rows_per_block) / rows_per_block + 1;
    if !all_constant {
        blocks_per_chunk = blocks_per_chunk.saturating_add(2);
    }
    let size_per_partition = blocks_per_chunk.saturating_mul(DEFAULT_BLOCK_ALLOC_SIZE);
    let partition_count = RadixPartitioning::number_of_partitions(radix_bits);
    num_threads
        .max(1)
        .saturating_mul(partition_count)
        .saturating_mul(size_per_partition)
}

pub(super) fn estimate_total_size_with_load_factor(
    data_size: usize,
    row_count: usize,
    load_factor: f64,
) -> usize {
    if row_count == 0 {
        return data_size;
    }
    let base = JoinHashTable::pointer_table_size_for_count(row_count);
    if load_factor <= 0.0 {
        return data_size.saturating_add(base);
    }
    let scaled = ((base as f64) / load_factor).ceil() as usize;
    data_size.saturating_add(scaled.max(std::mem::size_of::<usize>()))
}

fn compute_hashes_for_keys(keys: &Chunk, sel: Option<&SelectionVector>, count: usize) -> Vector {
    let mut hashes = Vector::with_capacity(LogicalType::UBigInt, count.max(1));
    hashes.set_count(count);
    for out_idx in 0..count {
        let row_idx = sel.map(|s| s.get(out_idx)).unwrap_or(out_idx);
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        for key_col_idx in 0..keys.column_count() {
            let value = keys.data[key_col_idx].get_value(row_idx);
            value.hash(&mut hasher);
        }
        hashes.set_u64(out_idx, hasher.finish());
    }
    hashes
}

pub(super) fn build_dictionary_chunk(
    input: &Chunk,
    sel: &SelectionVector,
    count: usize,
) -> Result<Chunk> {
    if count == input.size() {
        return Ok(input.clone());
    }
    let mut vectors = Vec::with_capacity(input.column_count());
    for col_idx in 0..input.column_count() {
        let source = input.column(col_idx).ok_or_else(|| {
            paro_error::internal(format!(
                "missing input column while slicing hash join chunk: column_idx={col_idx}"
            ))
        })?;
        vectors.push(Arc::new(Vector::dictionary(
            Arc::clone(source),
            sel.clone(),
        )));
    }
    let mut chunk = Chunk::from_arc_vectors(vectors);
    chunk.set_cardinality(count);
    Ok(chunk)
}

pub(super) fn materialize_chunk(chunk: &Chunk) -> Chunk {
    chunk.deep_copy_with_allocator(chunk.allocator().clone())
}

pub(super) fn external_keys_are_skewed(selected_partition_sizes: &[usize]) -> bool {
    if selected_partition_sizes.is_empty() {
        return false;
    }

    let total_size = selected_partition_sizes.iter().copied().sum::<usize>();
    if total_size == 0 {
        return false;
    }

    let max_partition_size = selected_partition_sizes.iter().copied().max().unwrap_or(0);
    (max_partition_size as f64) / (total_size as f64) > SKEW_SINGLE_THREADED_THRESHOLD
}

pub(super) fn build_probe_spill_chunk(input: &Chunk, hashes: &Vector) -> Result<Chunk> {
    let mut vectors = Vec::with_capacity(input.column_count().saturating_add(1));
    for col_idx in 0..input.column_count() {
        let column = input.column(col_idx).ok_or_else(|| {
            paro_error::internal(format!(
                "missing input column while building probe spill chunk: column_idx={col_idx}"
            ))
        })?;
        vectors.push(Arc::clone(column));
    }
    vectors.push(Arc::new(hashes.clone()));
    let mut chunk = Chunk::from_arc_vectors(vectors);
    chunk.set_cardinality(input.size());
    Ok(chunk)
}

pub(super) fn repartition_local_tables_into_sink_collection(
    local_tables: &[Arc<JoinHashTable>],
    sink_collection: &mut RadixPartitionedRowsBuilder,
) -> Result<bool> {
    let mut global_has_null = false;

    for local_ht in local_tables {
        global_has_null |= local_ht.has_null.load(std::sync::atomic::Ordering::Relaxed);
        local_ht.drain_build_store_spill_chunks(|spill_chunk| {
            sink_collection.append(spill_chunk).map_err(|err| {
                paro_error::internal(format!(
                    "failed to repartition hash join build rows into external sink collection: {err}"
                ))
            })
        })?;
    }

    Ok(global_has_null)
}

pub(super) fn prepare_external_build_round(
    join: &HashJoin,
    gstate: &HashJoinGlobalSinkState,
    runtime: &mut HashJoinExternalRuntime,
) -> Result<bool> {
    runtime.current_partitions.fill(false);
    if runtime.completed_partitions.iter().all(|done| *done) {
        gstate.temporary_memory_state.set_zero();
        return Ok(false);
    }

    gstate.hash_table.reset_runtime_state();
    gstate.hash_table.reset_data_collection();
    gstate.hash_table.set_has_null(runtime.global_has_null);

    let mut unfinished = Vec::new();
    let mut min_partition_size = usize::MAX;
    for partition_idx in 0..runtime.partition_count() {
        if runtime.completed_partitions[partition_idx] {
            continue;
        }
        let partition = runtime.sink_collection.partition(partition_idx);
        let size = JoinHashTable::estimate_total_size(
            partition.size_in_bytes(),
            partition.count() as usize,
        );
        min_partition_size = min_partition_size.min(size.max(1));
        unfinished.push((partition_idx, size));
    }

    if unfinished.is_empty() {
        gstate.temporary_memory_state.set_zero();
        return Ok(false);
    }

    unfinished.sort_by(|(l_idx, l_size), (r_idx, r_size)| {
        (l_size / min_partition_size)
            .cmp(&(r_size / min_partition_size))
            .then_with(|| l_idx.cmp(r_idx))
    });

    let reservation = gstate.temporary_memory_state.get_reservation();
    let max_ht_budget = reservation
        .saturating_sub(runtime.probe_side_requirement)
        .saturating_sub(EXTERNAL_BUILD_HEADROOM_BYTES)
        .max(1);
    let mut combined_count = 0usize;
    let mut combined_size = 0usize;
    let mut selected_partition_count = 0usize;
    let mut selected_partition_sizes = Vec::new();
    let mut spill_chunk = Chunk::new();

    let mut build_store = gstate.hash_table.get_build_store();
    for (partition_idx, _size) in unfinished {
        let partition_count = runtime.sink_collection.partition(partition_idx).count() as usize;
        let partition_data_size = runtime
            .sink_collection
            .partition(partition_idx)
            .size_in_bytes();
        let incl_count = combined_count.saturating_add(partition_count);
        let incl_size = combined_size.saturating_add(partition_data_size);
        let incl_ht_size = JoinHashTable::estimate_total_size(incl_size, incl_count);
        if combined_count > 0 && incl_ht_size > max_ht_budget {
            break;
        }

        runtime.current_partitions[partition_idx] = true;
        runtime.completed_partitions[partition_idx] = true;
        selected_partition_count = selected_partition_count.saturating_add(1);
        combined_count = incl_count;
        combined_size = incl_size;
        selected_partition_sizes.push(JoinHashTable::estimate_total_size(
            partition_data_size,
            partition_count,
        ));

        let partition = runtime.sink_collection.take_partition(partition_idx);
        let mut scanner = partition.scanner();
        loop {
            let scanned = scanner.next_chunk(&mut spill_chunk)?;
            if scanned == 0 {
                break;
            }
            build_store.append_chunk(&spill_chunk)?;
        }
    }
    drop(build_store);

    gstate.hash_table.refresh_count_from_data_collection();
    if gstate.hash_table.count() == 0 {
        gstate.temporary_memory_state.set_zero();
        return Ok(selected_partition_count > 0);
    }

    let ht_remaining = JoinHashTable::estimate_total_size(
        gstate.hash_table.build_rows_size_in_bytes(),
        gstate.hash_table.count(),
    );
    let reservation_target = ht_remaining.saturating_add(runtime.probe_side_requirement);
    if reservation_target == 0 {
        gstate.temporary_memory_state.set_zero();
    } else {
        gstate
            .temporary_memory_state
            .set_remaining_size_and_update_reservation(reservation_target);
    }

    let _single_thread_build = external_keys_are_skewed(&selected_partition_sizes)
        || gstate
            .num_threads
            .load(std::sync::atomic::Ordering::Acquire)
            <= 1;
    gstate.hash_table.finalize()?;
    let _ = join;
    Ok(true)
}

pub(super) fn split_probe_partitions(
    hashes: &Vector,
    radix_bits: usize,
    current_partitions: &[bool],
    count: usize,
) -> Result<(SelectionVector, usize, SelectionVector, usize)> {
    let mut true_sel = SelectionVector::with_capacity(count);
    true_sel.set_len(count);
    let mut false_sel = SelectionVector::with_capacity(count);
    false_sel.set_len(count);

    let mut true_count = 0usize;
    let mut false_count = 0usize;
    for row_idx in 0..count {
        let hash = hashes.get_u64(row_idx).ok_or_else(|| {
            paro_error::internal(format!(
                "hash join external probe hash is NULL at row {row_idx}"
            ))
        })?;
        let partition_idx = RadixPartitioning::apply_mask(hash, radix_bits);
        let is_current = current_partitions
            .get(partition_idx)
            .copied()
            .unwrap_or(false);
        if is_current {
            true_sel.set(true_count, row_idx);
            true_count = true_count.saturating_add(1);
        } else {
            false_sel.set(false_count, row_idx);
            false_count = false_count.saturating_add(1);
        }
    }

    true_sel.set_len(true_count);
    false_sel.set_len(false_count);
    Ok((true_sel, true_count, false_sel, false_count))
}

pub(super) fn append_external_source_rows(
    join: &HashJoin,
    gstate: &HashJoinGlobalSinkState,
    runtime: &mut HashJoinExternalRuntime,
    build_chunk: &Chunk,
) -> Result<()> {
    if build_chunk.size() == 0 {
        return Ok(());
    }
    if runtime.source_output_rows_builder.is_none() {
        runtime.source_output_rows_builder = Some(RowStoreBuilder::from_types(
            gstate.hash_table.buffer_pool().clone(),
            join.build_layout().build_payload_types.clone(),
            MemoryTag::HashTable,
        ));
    }
    if let Some(collection) = runtime.source_output_rows_builder.as_mut() {
        collection.append(build_chunk)?;
    }
    Ok(())
}

pub(super) fn finalize_external_source_rows(
    join: &HashJoin,
    gstate: &HashJoinGlobalSinkState,
    runtime: &mut HashJoinExternalRuntime,
) {
    if !join.base().join.is_source() {
        *gstate.external_source_rows.lock().unwrap() = None;
        return;
    }
    let rows = runtime
        .source_output_rows_builder
        .take()
        .map(RowStoreBuilder::seal);
    *gstate.external_source_rows.lock().unwrap() = rows;
    gstate.external_source_scan_state.lock().unwrap().reset();
}

pub(super) fn execute_external_probe(
    ctx: &ExecutionContext,
    join: &HashJoin,
    gsink: &HashJoinGlobalSinkState,
    state: &mut HashJoinOperatorState,
    input: &Chunk,
    chunk: &mut Chunk,
) -> Result<OperatorResultType> {
    let ht = &gsink.hash_table;
    let hashes = compute_hashes_for_keys(&state.probe_keys, None, input.size());
    let (current_partitions, radix_bits) = {
        let runtime_guard = gsink.external_runtime.lock().unwrap();
        let runtime = runtime_guard.as_ref().ok_or_else(|| {
            paro_error::internal(
                "hash join externalized but runtime is missing in execute".to_string(),
            )
        })?;
        (runtime.current_partitions.clone(), runtime.radix_bits)
    };

    if state.external_probe_local_state.is_none() {
        let mut runtime_guard = gsink.external_runtime.lock().unwrap();
        let runtime = runtime_guard.as_mut().ok_or_else(|| {
            paro_error::internal(
                "hash join externalized but runtime is missing while registering probe spill local state"
                    .to_string(),
            )
        })?;
        state.external_probe_local_state = Some(runtime.probe_spill.register_thread());
    }

    let (true_sel, true_count, false_sel, false_count) =
        split_probe_partitions(&hashes, radix_bits, &current_partitions, input.size())?;

    if false_count > 0 {
        let false_input = build_dictionary_chunk(input, &false_sel, false_count)?;
        let false_hashes = Vector::dictionary(Arc::new(hashes.clone()), false_sel);
        let false_chunk = build_probe_spill_chunk(&false_input, &false_hashes)?;

        let mut runtime_guard = gsink.external_runtime.lock().unwrap();
        let runtime = runtime_guard.as_mut().ok_or_else(|| {
            paro_error::internal(
                "hash join externalized but runtime disappeared while appending probe spill chunk"
                    .to_string(),
            )
        })?;
        let local_state = state.external_probe_local_state.as_mut().ok_or_else(|| {
            paro_error::internal("probe spill local state not initialized".to_string())
        })?;
        runtime.probe_spill.append(&false_chunk, local_state)?;
    }

    if true_count == 0 {
        state.scan_structure.reset();
        state.probe_in_progress = false;
        state.current_probe_input.reset();
        chunk.set_cardinality(0);
        return Ok(OperatorResultType::NeedMoreInput);
    }

    let true_input = build_dictionary_chunk(input, &true_sel, true_count)?;
    let true_probe_keys = build_dictionary_chunk(&state.probe_keys, &true_sel, true_count)?;
    state.probe_keys = true_probe_keys;
    state.current_probe_input = materialize_chunk(&true_input);

    ht.probe(
        &state.probe_keys,
        &mut state.scan_structure,
        None,
        true_count,
    );
    state.probe_in_progress = true;

    let count = probe_engine::scan_join_results(
        ctx,
        join.base().join.join_type,
        &state.probe_keys,
        &state.current_probe_input,
        chunk,
        &mut state.scan_structure,
        ht,
        &join.base().join.left_projection_map,
        &join.build_layout().right_projection_map_for_build,
        &join.build_layout().residual_conditions_on_build_payload,
        &mut state.residual_condition_executors,
        &join.build_layout().build_payload_types,
    )?;
    state.probe_in_progress = !state.scan_structure.finished;
    if state.scan_structure.finished {
        state.current_probe_input.reset();
    }
    Ok(probe_engine::result_for_probe_batch(
        count,
        state.scan_structure.finished,
    ))
}

pub(super) fn drive_external_replay(
    ctx: &ExecutionContext,
    join: &HashJoin,
    gsink: &HashJoinGlobalSinkState,
    state: &mut HashJoinOperatorState,
    chunk: &mut Chunk,
) -> Result<OperatorFinalizeResultType> {
    let ht = &gsink.hash_table;
    probe_engine::prepare_output_chunk(
        chunk,
        &join.base().join.types,
        paro_common::vector::VECTOR_SIZE,
    );

    loop {
        if state.probe_in_progress {
            let count = probe_engine::scan_join_results(
                ctx,
                join.base().join.join_type,
                &state.probe_keys,
                &state.current_probe_input,
                chunk,
                &mut state.scan_structure,
                ht,
                &join.base().join.left_projection_map,
                &join.build_layout().right_projection_map_for_build,
                &join.build_layout().residual_conditions_on_build_payload,
                &mut state.residual_condition_executors,
                &join.build_layout().build_payload_types,
            )?;
            state.probe_in_progress = !state.scan_structure.finished;
            if state.scan_structure.finished {
                state.current_probe_input.reset();
            }
            if count > 0 {
                return Ok(OperatorFinalizeResultType::HaveMoreOutput);
            }
            if state.probe_in_progress {
                return Ok(OperatorFinalizeResultType::HaveMoreOutput);
            }
        }

        let mut runtime_guard = gsink.external_runtime.lock().unwrap();
        let runtime = runtime_guard.as_mut().ok_or_else(|| {
            paro_error::internal("hash join externalized but runtime is missing".to_string())
        })?;

        match runtime.probe_stage {
            HashJoinExternalProbeStage::PrepareBuild => {
                if !runtime.probe_spill_finalized {
                    runtime.probe_spill.finalize()?;
                    runtime.probe_spill_finalized = true;
                }

                let prepared = prepare_external_build_round(join, gsink, runtime)?;
                if !prepared {
                    runtime.probe_stage = HashJoinExternalProbeStage::Done;
                    continue;
                }

                runtime
                    .probe_spill
                    .set_current_partitions(runtime.current_partitions.clone())?;
                runtime.probe_rows = runtime.probe_spill.prepare_next_probe()?;
                runtime.probe_stage = HashJoinExternalProbeStage::Probe;
                state.external_probe_scan_state = RowScanState::default();
                state.external_build_scan_state = None;
            }
            HashJoinExternalProbeStage::Probe => {
                if !runtime.probe_spill_finalized {
                    runtime.probe_spill.finalize()?;
                    runtime.probe_spill_finalized = true;
                }

                if runtime.probe_rows.is_none() {
                    runtime
                        .probe_spill
                        .set_current_partitions(runtime.current_partitions.clone())?;
                    runtime.probe_rows = runtime.probe_spill.prepare_next_probe()?;
                    state.external_probe_scan_state.reset();
                }

                let Some(probe_rows) = runtime.probe_rows.as_ref() else {
                    runtime.probe_stage = if join.base().join.is_source() {
                        HashJoinExternalProbeStage::ScanBuild
                    } else {
                        HashJoinExternalProbeStage::PrepareBuild
                    };
                    continue;
                };

                if state.external_probe_chunk.column_count() == 0 {
                    let mut probe_types = join.base().join.left.types().to_vec();
                    probe_types.push(LogicalType::UBigInt);
                    state.external_probe_chunk =
                        Chunk::initialize(&probe_types, paro_common::vector::VECTOR_SIZE);
                }

                let scanned = probe_rows.scan_with_state(
                    &mut state.external_probe_scan_state,
                    &mut state.external_probe_chunk,
                )?;
                if scanned == 0 {
                    runtime.probe_rows = None;
                    state.external_probe_scan_state.reset();
                    runtime.probe_stage = if join.base().join.is_source() {
                        HashJoinExternalProbeStage::ScanBuild
                    } else {
                        HashJoinExternalProbeStage::PrepareBuild
                    };
                    continue;
                }
                state.external_probe_chunk.set_cardinality(scanned);
                let replay_input = {
                    let probe_col_count = join.base().join.left.types().len();
                    let mut replay_chunk = Chunk::from_arc_vectors(
                        state.external_probe_chunk.data[..probe_col_count].to_vec(),
                    );
                    replay_chunk.set_cardinality(scanned);
                    replay_chunk
                };
                drop(runtime_guard);

                if ht.is_empty() {
                    if join.base().join.empty_result_if_rhs_is_empty() {
                        chunk.set_cardinality(0);
                    } else {
                        join.base().construct_empty_join_result(
                            &replay_input,
                            chunk,
                            ht.has_null.load(std::sync::atomic::Ordering::Relaxed),
                        );
                    }

                    if chunk.size() > 0 {
                        return Ok(OperatorFinalizeResultType::HaveMoreOutput);
                    }
                    continue;
                }

                state.probe_keys = probe_engine::evaluate_probe_keys(
                    ctx,
                    &replay_input,
                    &join.base().equality_conditions,
                    &mut state.probe_key_executors,
                )?;
                state.current_probe_input = replay_input;
                ht.probe(
                    &state.probe_keys,
                    &mut state.scan_structure,
                    None,
                    state.probe_keys.size(),
                );
                state.probe_in_progress = true;
            }
            HashJoinExternalProbeStage::ScanBuild => {
                if !join.base().join.is_source() {
                    runtime.probe_stage = HashJoinExternalProbeStage::PrepareBuild;
                    continue;
                }
                if state.external_build_scan_state.is_none() {
                    state.external_build_scan_state =
                        Some(gsink.hash_table.create_full_outer_scan_state());
                }
                if state.external_build_chunk.column_count() == 0 {
                    state.external_build_chunk = Chunk::initialize(
                        &join.build_layout().build_payload_types,
                        paro_common::vector::VECTOR_SIZE,
                    );
                }
                let emit_found = matches!(join.base().join.join_type, JoinType::RightSemi);
                let scan_state = state.external_build_scan_state.as_mut().ok_or_else(|| {
                    paro_error::internal(
                        "hash join external SCAN_HT stage missing build scan state".to_string(),
                    )
                })?;

                drop(runtime_guard);
                let scanned = gsink.hash_table.scan_full_outer(
                    scan_state,
                    emit_found,
                    &mut state.external_build_chunk,
                )?;
                if scanned == 0 {
                    state.external_build_scan_state = None;
                    let mut runtime_guard = gsink.external_runtime.lock().unwrap();
                    if let Some(runtime) = runtime_guard.as_mut() {
                        runtime.probe_stage = HashJoinExternalProbeStage::PrepareBuild;
                    }
                    continue;
                }

                let mut runtime_guard = gsink.external_runtime.lock().unwrap();
                let runtime = runtime_guard.as_mut().ok_or_else(|| {
                    paro_error::internal(
                        "hash join external runtime missing while appending SCAN_HT rows"
                            .to_string(),
                    )
                })?;
                append_external_source_rows(join, gsink, runtime, &state.external_build_chunk)?;
            }
            HashJoinExternalProbeStage::Done => {
                let keep_source_rows = join.base().join.is_source();
                finalize_external_source_rows(join, gsink, runtime);
                drop(runtime_guard);
                gsink.cleanup_external_spill_state(!keep_source_rows);
                return Ok(OperatorFinalizeResultType::Finished);
            }
        }
    }
}

pub(super) fn get_data_external(
    join: &HashJoin,
    gsink: &HashJoinGlobalSinkState,
    _lstate: &mut HashJoinLocalSourceState,
    chunk: &mut Chunk,
) -> Result<SourceResultType> {
    let source_rows_guard = gsink.external_source_rows.lock().unwrap();
    let Some(source_rows) = source_rows_guard.as_ref() else {
        chunk.set_cardinality(0);
        gsink.cleanup_external_spill_state(true);
        return Ok(SourceResultType::Finished);
    };

    probe_engine::prepare_output_chunk(
        chunk,
        &join.base().join.types,
        paro_common::vector::VECTOR_SIZE,
    );

    let mut build_chunk = Chunk::initialize(
        &join.build_layout().build_payload_types,
        paro_common::vector::VECTOR_SIZE,
    );
    let mut scan_state = gsink.external_source_scan_state.lock().unwrap();
    let scanned = source_rows.scan_with_state(&mut scan_state, &mut build_chunk)?;
    drop(scan_state);
    drop(source_rows_guard);

    if scanned == 0 {
        chunk.set_cardinality(0);
        gsink.cleanup_external_spill_state(true);
        return Ok(SourceResultType::Finished);
    }

    let build_sel = SelectionVector::incremental(scanned);
    match join.base().join.join_type {
        JoinType::Right | JoinType::Outer => construct_right_outer_scan_result(
            &build_chunk,
            &build_sel,
            scanned,
            &join.base().join.left_output_types,
            &join.build_layout().right_projection_map_for_build,
            chunk,
        ),
        JoinType::RightSemi | JoinType::RightAnti => construct_semi_join_result(
            &build_chunk,
            &build_sel,
            scanned,
            &join.build_layout().right_projection_map_for_build,
            chunk,
        ),
        _ => {
            chunk.set_cardinality(0);
            return Ok(SourceResultType::Finished);
        }
    }

    Ok(SourceResultType::HaveMoreOutput)
}

#[cfg(test)]
mod tests {
    use super::external_keys_are_skewed;

    #[test]
    fn external_keys_are_skewed_detects_dominant_partition() {
        assert!(external_keys_are_skewed(&[90, 5, 5]));
        assert!(!external_keys_are_skewed(&[25, 25, 25, 25]));
        assert!(!external_keys_are_skewed(&[]));
    }
}
