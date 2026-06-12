// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! paro_transaction_metrics() table function.

use std::any::Any;
use std::sync::atomic::{AtomicUsize, Ordering};

use paro_common::chunk::Chunk;
use paro_common::error::Result;
use paro_common::types::LogicalType;
use paro_common::vector::Vector;

use crate::table::{
    GlobalTableFunctionState, TableFunction, TableFunctionBindData, TableFunctionBindInput,
    TableFunctionInitInput, TableFunctionInput, TableFunctionResult, TableFunctionSet,
};

#[derive(Clone)]
pub struct ParoTransactionMetricsBindData;

impl TableFunctionBindData for ParoTransactionMetricsBindData {
    fn clone_box(&self) -> Box<dyn TableFunctionBindData> {
        Box::new(self.clone())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn cardinality(&self) -> Option<usize> {
        None
    }
}

#[derive(Debug, Clone)]
pub struct TransactionMetricData {
    pub database_oid: u64,
    pub database_name: String,
    pub txn_begin_count: i64,
    pub txn_begin_latency_us_total: i64,
    pub txn_begin_latency_us_peak: i64,
    pub txn_commit_count: i64,
    pub txn_commit_latency_us_total: i64,
    pub txn_commit_latency_us_peak: i64,
    pub txn_commit_prepare_latency_us_total: i64,
    pub txn_commit_prepare_latency_us_peak: i64,
    pub txn_commit_validate_latency_us_total: i64,
    pub txn_commit_validate_latency_us_peak: i64,
    pub group_commit_fence_us_total: i64,
    pub group_commit_fence_us_peak: i64,
    pub txn_commit_durable_latency_us_total: i64,
    pub txn_commit_durable_latency_us_peak: i64,
    pub commit_required_publish_wait_us_total: i64,
    pub commit_required_publish_wait_us_peak: i64,
    pub txn_commit_publish_latency_us_total: i64,
    pub txn_commit_publish_latency_us_peak: i64,
    pub commit_ack_mode: String,
    pub write_conflict_index_size: i64,
    pub write_conflict_index_fine_entries: i64,
    pub write_conflict_index_fine_summary_entries: i64,
    pub write_conflict_index_coarse_entries: i64,
    pub lock_wait_count: i64,
    pub lock_wait_duration_us: i64,
    pub lock_wound_wait_abort_count: i64,
    pub lock_deadlock_abort_count: i64,
    pub durable_published_lag_commits: i64,
    pub durable_published_lag_ms: i64,
    pub backpressure_throttle_count: i64,
    pub ssi_validation_abort_count: i64,
    pub ssi_abort_due_to_coarse_scan_marker: i64,
    pub read_tracker_record_count: i64,
    pub read_tracker_coarsened_count: i64,
    pub read_tracking_hint_count: i64,
    pub read_tracking_policy_escalation_count: i64,
    pub read_tracking_point_critical_count: i64,
    pub read_tracking_range_critical_count: i64,
    pub read_tracking_analytical_scan_count: i64,
    pub read_tracking_safe_snapshot_preferred_count: i64,
    pub derived_index_lag_ts: i64,
    pub tail_exact_merge_cost: i64,
    pub commit_participant_count: i64,
    pub inflight_batch_conflict_reject_count: i64,
    pub retention_watermark_lag_ms: i64,
    pub oldest_active_rw_lag_ms: i64,
    pub read_snapshot_lease_count: i64,
    pub active_rw_txn_count: i64,
}

pub struct ParoTransactionMetricsGlobalState {
    pub entries: Vec<TransactionMetricData>,
    pub offset: AtomicUsize,
}

impl GlobalTableFunctionState for ParoTransactionMetricsGlobalState {
    fn max_threads(&self) -> usize {
        1
    }

    fn get_progress(&self) -> f64 {
        if self.entries.is_empty() {
            return 100.0;
        }
        let offset = self.offset.load(Ordering::Relaxed);
        (offset as f64 / self.entries.len() as f64) * 100.0
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

fn paro_transaction_metrics_bind(
    _input: &TableFunctionBindInput,
    return_types: &mut Vec<LogicalType>,
    names: &mut Vec<String>,
) -> Result<Option<Box<dyn TableFunctionBindData>>> {
    for (name, ty) in [
        ("database_oid", LogicalType::BigInt),
        ("database_name", LogicalType::Varchar),
        ("txn_begin_count", LogicalType::BigInt),
        ("txn_begin_latency_us_total", LogicalType::BigInt),
        ("txn_begin_latency_us_peak", LogicalType::BigInt),
        ("txn_commit_count", LogicalType::BigInt),
        ("txn_commit_latency_us_total", LogicalType::BigInt),
        ("txn_commit_latency_us_peak", LogicalType::BigInt),
        ("txn_commit_prepare_latency_us_total", LogicalType::BigInt),
        ("txn_commit_prepare_latency_us_peak", LogicalType::BigInt),
        ("txn_commit_validate_latency_us_total", LogicalType::BigInt),
        ("txn_commit_validate_latency_us_peak", LogicalType::BigInt),
        ("group_commit_fence_us_total", LogicalType::BigInt),
        ("group_commit_fence_us_peak", LogicalType::BigInt),
        ("txn_commit_durable_latency_us_total", LogicalType::BigInt),
        ("txn_commit_durable_latency_us_peak", LogicalType::BigInt),
        ("commit_required_publish_wait_us_total", LogicalType::BigInt),
        ("commit_required_publish_wait_us_peak", LogicalType::BigInt),
        ("txn_commit_publish_latency_us_total", LogicalType::BigInt),
        ("txn_commit_publish_latency_us_peak", LogicalType::BigInt),
        ("commit_ack_mode", LogicalType::Varchar),
        ("write_conflict_index_size", LogicalType::BigInt),
        ("write_conflict_index_fine_entries", LogicalType::BigInt),
        (
            "write_conflict_index_fine_summary_entries",
            LogicalType::BigInt,
        ),
        ("write_conflict_index_coarse_entries", LogicalType::BigInt),
        ("lock_wait_count", LogicalType::BigInt),
        ("lock_wait_duration_us", LogicalType::BigInt),
        ("lock_wound_wait_abort_count", LogicalType::BigInt),
        ("lock_deadlock_abort_count", LogicalType::BigInt),
        ("durable_published_lag_commits", LogicalType::BigInt),
        ("durable_published_lag_ms", LogicalType::BigInt),
        ("backpressure_throttle_count", LogicalType::BigInt),
        ("ssi_validation_abort_count", LogicalType::BigInt),
        ("ssi_abort_due_to_coarse_scan_marker", LogicalType::BigInt),
        ("read_tracker_record_count", LogicalType::BigInt),
        ("read_tracker_coarsened_count", LogicalType::BigInt),
        ("read_tracking_hint_count", LogicalType::BigInt),
        ("read_tracking_policy_escalation_count", LogicalType::BigInt),
        ("read_tracking_point_critical_count", LogicalType::BigInt),
        ("read_tracking_range_critical_count", LogicalType::BigInt),
        ("read_tracking_analytical_scan_count", LogicalType::BigInt),
        (
            "read_tracking_safe_snapshot_preferred_count",
            LogicalType::BigInt,
        ),
        ("derived_index_lag_ts", LogicalType::BigInt),
        ("tail_exact_merge_cost", LogicalType::BigInt),
        ("commit_participant_count", LogicalType::BigInt),
        ("inflight_batch_conflict_reject_count", LogicalType::BigInt),
        ("retention_watermark_lag_ms", LogicalType::BigInt),
        ("oldest_active_rw_lag_ms", LogicalType::BigInt),
        ("read_snapshot_lease_count", LogicalType::BigInt),
        ("active_rw_txn_count", LogicalType::BigInt),
    ] {
        names.push(name.to_string());
        return_types.push(ty);
    }

    Ok(Some(Box::new(ParoTransactionMetricsBindData)))
}

fn paro_transaction_metrics_init_global(
    _input: &TableFunctionInitInput,
) -> Result<Option<Box<dyn GlobalTableFunctionState>>> {
    Ok(Some(Box::new(ParoTransactionMetricsGlobalState {
        entries: Vec::new(),
        offset: AtomicUsize::new(0),
    })))
}

fn paro_transaction_metrics_function(
    input: &mut TableFunctionInput,
    output: &mut Chunk,
) -> Result<TableFunctionResult> {
    let output_allocator = output.allocator().clone();
    let gstate = input.global_state.and_then(|state| {
        state
            .as_any()
            .downcast_ref::<ParoTransactionMetricsGlobalState>()
    });
    let Some(gstate) = gstate else {
        output.set_cardinality(0);
        return Ok(TableFunctionResult::Finished);
    };

    let offset = gstate.offset.load(Ordering::Relaxed);
    if offset >= gstate.entries.len() {
        output.set_cardinality(0);
        return Ok(TableFunctionResult::Finished);
    }

    let batch_size = 2048.min(gstate.entries.len() - offset);
    let mut database_oids = Vec::with_capacity(batch_size);
    let mut database_names = Vec::with_capacity(batch_size);
    let mut ack_modes = Vec::with_capacity(batch_size);
    let mut columns: [Vec<i64>; 47] = std::array::from_fn(|_| Vec::with_capacity(batch_size));

    for entry in gstate.entries.iter().skip(offset).take(batch_size) {
        database_oids.push(entry.database_oid.min(i64::MAX as u64) as i64);
        database_names.push(entry.database_name.clone());
        ack_modes.push(entry.commit_ack_mode.clone());
        for (slot, value) in [
            entry.txn_begin_count,
            entry.txn_begin_latency_us_total,
            entry.txn_begin_latency_us_peak,
            entry.txn_commit_count,
            entry.txn_commit_latency_us_total,
            entry.txn_commit_latency_us_peak,
            entry.txn_commit_prepare_latency_us_total,
            entry.txn_commit_prepare_latency_us_peak,
            entry.txn_commit_validate_latency_us_total,
            entry.txn_commit_validate_latency_us_peak,
            entry.group_commit_fence_us_total,
            entry.group_commit_fence_us_peak,
            entry.txn_commit_durable_latency_us_total,
            entry.txn_commit_durable_latency_us_peak,
            entry.commit_required_publish_wait_us_total,
            entry.commit_required_publish_wait_us_peak,
            entry.txn_commit_publish_latency_us_total,
            entry.txn_commit_publish_latency_us_peak,
            entry.write_conflict_index_size,
            entry.write_conflict_index_fine_entries,
            entry.write_conflict_index_fine_summary_entries,
            entry.write_conflict_index_coarse_entries,
            entry.lock_wait_count,
            entry.lock_wait_duration_us,
            entry.lock_wound_wait_abort_count,
            entry.lock_deadlock_abort_count,
            entry.durable_published_lag_commits,
            entry.durable_published_lag_ms,
            entry.backpressure_throttle_count,
            entry.ssi_validation_abort_count,
            entry.ssi_abort_due_to_coarse_scan_marker,
            entry.read_tracker_record_count,
            entry.read_tracker_coarsened_count,
            entry.read_tracking_hint_count,
            entry.read_tracking_policy_escalation_count,
            entry.read_tracking_point_critical_count,
            entry.read_tracking_range_critical_count,
            entry.read_tracking_analytical_scan_count,
            entry.read_tracking_safe_snapshot_preferred_count,
            entry.derived_index_lag_ts,
            entry.tail_exact_merge_cost,
            entry.commit_participant_count,
            entry.inflight_batch_conflict_reject_count,
            entry.retention_watermark_lag_ms,
            entry.oldest_active_rw_lag_ms,
            entry.read_snapshot_lease_count,
            entry.active_rw_txn_count,
        ]
        .into_iter()
        .enumerate()
        {
            columns[slot].push(value);
        }
    }

    gstate.offset.fetch_add(batch_size, Ordering::Relaxed);

    let database_name_refs = database_names
        .iter()
        .map(|value| value.as_str())
        .collect::<Vec<_>>();
    let ack_mode_refs = ack_modes
        .iter()
        .map(|value| value.as_str())
        .collect::<Vec<_>>();

    if let Some(col) = output.column_mut(0) {
        *col = Vector::try_from_i64(&database_oids, output_allocator.clone())?;
    }
    if let Some(col) = output.column_mut(1) {
        *col = Vector::try_from_strings(&database_name_refs, output_allocator.clone())?;
    }
    for (slot, values) in columns.iter().take(18).enumerate() {
        if let Some(col) = output.column_mut(slot + 2) {
            *col = Vector::try_from_i64(values, output_allocator.clone())?;
        }
    }
    if let Some(col) = output.column_mut(20) {
        *col = Vector::try_from_strings(&ack_mode_refs, output_allocator.clone())?;
    }
    for (slot, values) in columns.iter().skip(18).enumerate() {
        if let Some(col) = output.column_mut(slot + 21) {
            *col = Vector::try_from_i64(values, output_allocator.clone())?;
        }
    }

    output.set_cardinality(batch_size);
    if gstate.offset.load(Ordering::Relaxed) >= gstate.entries.len() {
        Ok(TableFunctionResult::Finished)
    } else {
        Ok(TableFunctionResult::HaveMoreOutput)
    }
}

fn paro_transaction_metrics_progress(
    _bind_data: Option<&dyn TableFunctionBindData>,
    global_state: Option<&dyn GlobalTableFunctionState>,
) -> f64 {
    global_state
        .map(|state| state.get_progress())
        .unwrap_or(-1.0)
}

pub fn create_paro_transaction_metrics_function_set() -> TableFunctionSet {
    let mut func = TableFunction::new("paro_transaction_metrics", vec![]);
    func.bind = Some(paro_transaction_metrics_bind);
    func.init_global = Some(paro_transaction_metrics_init_global);
    func.function = Some(paro_transaction_metrics_function);
    func.table_scan_progress = Some(paro_transaction_metrics_progress);

    let mut set = TableFunctionSet::new("paro_transaction_metrics");
    set.add_function(func);
    set
}

pub fn populate_transaction_metric_data(
    state: &mut ParoTransactionMetricsGlobalState,
    entries: Vec<TransactionMetricData>,
) {
    state.entries = entries;
    state.offset.store(0, Ordering::Relaxed);
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_paro_transaction_metrics_bind() {
        let mut return_types = Vec::new();
        let mut names = Vec::new();

        let bind = paro_transaction_metrics_bind(
            &TableFunctionBindInput::new(&[], &HashMap::new()),
            &mut return_types,
            &mut names,
        )
        .unwrap();

        assert!(bind.is_some());
        assert_eq!(names[0], "database_oid");
        assert_eq!(names[20], "commit_ack_mode");
        assert_eq!(names[26], "lock_wait_duration_us");
        assert_eq!(names[41], "read_tracking_safe_snapshot_preferred_count");
        assert_eq!(names[49], "active_rw_txn_count");
        assert_eq!(return_types.len(), 50);
    }
}
