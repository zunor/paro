// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! paro_wal_metrics() Table Function

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
pub struct ParoWalMetricsBindData;

impl TableFunctionBindData for ParoWalMetricsBindData {
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
pub struct WalMetricData {
    pub database_oid: u64,
    pub database_name: String,
    pub recovery_mode: String,
    pub checkpoint_success_total: i64,
    pub checkpoint_failure_total: i64,
    pub wal_health_check_total: i64,
    pub wal_keep_from: i64,
    pub main_wal_needs_truncation: bool,
    pub checkpoint_wal_needs_truncation: bool,
    pub recovery_wal_needs_truncation: bool,
    pub journal_apply_queue_depth: i64,
    pub journal_apply_queue_depth_peak: i64,
    pub journal_apply_active_workers: i64,
    pub journal_apply_active_workers_peak: i64,
    pub journal_apply_mailbox_count: i64,
    pub journal_apply_applied_lag: i64,
    pub journal_apply_published_lag: i64,
    pub journal_apply_durable_wait_count: i64,
    pub journal_apply_durable_wait_micros: i64,
    pub journal_apply_applied_wait_count: i64,
    pub journal_apply_applied_wait_micros: i64,
    pub journal_apply_published_wait_count: i64,
    pub journal_apply_published_wait_micros: i64,
    pub journal_commit_bytes_total: i64,
    pub journal_group_count: i64,
    pub journal_group_size_last: i64,
    pub journal_group_size_peak: i64,
    pub journal_sync_latency_micros_total: i64,
    pub journal_sync_latency_micros_peak: i64,
    pub journal_replay_rowsets_total: i64,
    pub journal_replay_delete_patches_total: i64,
    pub journal_inline_patch_ratio: f64,
}

pub struct ParoWalMetricsGlobalState {
    pub entries: Vec<WalMetricData>,
    pub offset: AtomicUsize,
}

impl GlobalTableFunctionState for ParoWalMetricsGlobalState {
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

fn paro_wal_metrics_bind(
    _input: &TableFunctionBindInput,
    return_types: &mut Vec<LogicalType>,
    names: &mut Vec<String>,
) -> Result<Option<Box<dyn TableFunctionBindData>>> {
    for (name, ty) in [
        ("database_oid", LogicalType::BigInt),
        ("database_name", LogicalType::Varchar),
        ("recovery_mode", LogicalType::Varchar),
        ("checkpoint_success_total", LogicalType::BigInt),
        ("checkpoint_failure_total", LogicalType::BigInt),
        ("wal_health_check_total", LogicalType::BigInt),
        ("wal_keep_from", LogicalType::BigInt),
        ("main_wal_needs_truncation", LogicalType::Boolean),
        ("checkpoint_wal_needs_truncation", LogicalType::Boolean),
        ("recovery_wal_needs_truncation", LogicalType::Boolean),
        ("journal_apply_queue_depth", LogicalType::BigInt),
        ("journal_apply_queue_depth_peak", LogicalType::BigInt),
        ("journal_apply_active_workers", LogicalType::BigInt),
        ("journal_apply_active_workers_peak", LogicalType::BigInt),
        ("journal_apply_mailbox_count", LogicalType::BigInt),
        ("journal_apply_applied_lag", LogicalType::BigInt),
        ("journal_apply_published_lag", LogicalType::BigInt),
        ("journal_apply_durable_wait_count", LogicalType::BigInt),
        ("journal_apply_durable_wait_micros", LogicalType::BigInt),
        ("journal_apply_applied_wait_count", LogicalType::BigInt),
        ("journal_apply_applied_wait_micros", LogicalType::BigInt),
        ("journal_apply_published_wait_count", LogicalType::BigInt),
        ("journal_apply_published_wait_micros", LogicalType::BigInt),
        ("journal_commit_bytes_total", LogicalType::BigInt),
        ("journal_group_count", LogicalType::BigInt),
        ("journal_group_size_last", LogicalType::BigInt),
        ("journal_group_size_peak", LogicalType::BigInt),
        ("journal_sync_latency_micros_total", LogicalType::BigInt),
        ("journal_sync_latency_micros_peak", LogicalType::BigInt),
        ("journal_replay_rowsets_total", LogicalType::BigInt),
        ("journal_replay_delete_patches_total", LogicalType::BigInt),
        ("journal_inline_patch_ratio", LogicalType::Double),
    ] {
        names.push(name.to_string());
        return_types.push(ty);
    }

    Ok(Some(Box::new(ParoWalMetricsBindData)))
}

fn paro_wal_metrics_init_global(
    _input: &TableFunctionInitInput,
) -> Result<Option<Box<dyn GlobalTableFunctionState>>> {
    Ok(Some(Box::new(ParoWalMetricsGlobalState {
        entries: Vec::new(),
        offset: AtomicUsize::new(0),
    })))
}

fn paro_wal_metrics_function(
    input: &mut TableFunctionInput,
    output: &mut Chunk,
) -> Result<TableFunctionResult> {
    let output_allocator = output.allocator().clone();
    let gstate = input
        .global_state
        .and_then(|gs| gs.as_any().downcast_ref::<ParoWalMetricsGlobalState>());
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
    let mut recovery_modes = Vec::with_capacity(batch_size);
    let mut checkpoint_success = Vec::with_capacity(batch_size);
    let mut checkpoint_failure = Vec::with_capacity(batch_size);
    let mut wal_health_checks = Vec::with_capacity(batch_size);
    let mut wal_keep_from = Vec::with_capacity(batch_size);
    let mut main_truncate = Vec::with_capacity(batch_size);
    let mut checkpoint_truncate = Vec::with_capacity(batch_size);
    let mut recovery_truncate = Vec::with_capacity(batch_size);
    let mut queue_depth = Vec::with_capacity(batch_size);
    let mut queue_depth_peak = Vec::with_capacity(batch_size);
    let mut active_workers = Vec::with_capacity(batch_size);
    let mut active_workers_peak = Vec::with_capacity(batch_size);
    let mut mailbox_count = Vec::with_capacity(batch_size);
    let mut applied_lag = Vec::with_capacity(batch_size);
    let mut published_lag = Vec::with_capacity(batch_size);
    let mut durable_wait_count = Vec::with_capacity(batch_size);
    let mut durable_wait_micros = Vec::with_capacity(batch_size);
    let mut applied_wait_count = Vec::with_capacity(batch_size);
    let mut applied_wait_micros = Vec::with_capacity(batch_size);
    let mut published_wait_count = Vec::with_capacity(batch_size);
    let mut published_wait_micros = Vec::with_capacity(batch_size);
    let mut commit_bytes_total = Vec::with_capacity(batch_size);
    let mut group_count = Vec::with_capacity(batch_size);
    let mut group_size_last = Vec::with_capacity(batch_size);
    let mut group_size_peak = Vec::with_capacity(batch_size);
    let mut sync_latency_total = Vec::with_capacity(batch_size);
    let mut sync_latency_peak = Vec::with_capacity(batch_size);
    let mut replay_rowsets_total = Vec::with_capacity(batch_size);
    let mut replay_delete_patches_total = Vec::with_capacity(batch_size);
    let mut inline_patch_ratio = Vec::with_capacity(batch_size);

    for entry in gstate.entries.iter().skip(offset).take(batch_size) {
        database_oids.push(entry.database_oid.min(i64::MAX as u64) as i64);
        database_names.push(entry.database_name.clone());
        recovery_modes.push(entry.recovery_mode.clone());
        checkpoint_success.push(entry.checkpoint_success_total);
        checkpoint_failure.push(entry.checkpoint_failure_total);
        wal_health_checks.push(entry.wal_health_check_total);
        wal_keep_from.push(entry.wal_keep_from);
        main_truncate.push(entry.main_wal_needs_truncation);
        checkpoint_truncate.push(entry.checkpoint_wal_needs_truncation);
        recovery_truncate.push(entry.recovery_wal_needs_truncation);
        queue_depth.push(entry.journal_apply_queue_depth);
        queue_depth_peak.push(entry.journal_apply_queue_depth_peak);
        active_workers.push(entry.journal_apply_active_workers);
        active_workers_peak.push(entry.journal_apply_active_workers_peak);
        mailbox_count.push(entry.journal_apply_mailbox_count);
        applied_lag.push(entry.journal_apply_applied_lag);
        published_lag.push(entry.journal_apply_published_lag);
        durable_wait_count.push(entry.journal_apply_durable_wait_count);
        durable_wait_micros.push(entry.journal_apply_durable_wait_micros);
        applied_wait_count.push(entry.journal_apply_applied_wait_count);
        applied_wait_micros.push(entry.journal_apply_applied_wait_micros);
        published_wait_count.push(entry.journal_apply_published_wait_count);
        published_wait_micros.push(entry.journal_apply_published_wait_micros);
        commit_bytes_total.push(entry.journal_commit_bytes_total);
        group_count.push(entry.journal_group_count);
        group_size_last.push(entry.journal_group_size_last);
        group_size_peak.push(entry.journal_group_size_peak);
        sync_latency_total.push(entry.journal_sync_latency_micros_total);
        sync_latency_peak.push(entry.journal_sync_latency_micros_peak);
        replay_rowsets_total.push(entry.journal_replay_rowsets_total);
        replay_delete_patches_total.push(entry.journal_replay_delete_patches_total);
        inline_patch_ratio.push(entry.journal_inline_patch_ratio);
    }

    gstate.offset.fetch_add(batch_size, Ordering::Relaxed);

    let database_name_refs = database_names
        .iter()
        .map(|value| value.as_str())
        .collect::<Vec<_>>();
    let recovery_mode_refs = recovery_modes
        .iter()
        .map(|value| value.as_str())
        .collect::<Vec<_>>();

    if let Some(col) = output.column_mut(0) {
        *col = Vector::try_from_i64(&database_oids, output_allocator.clone())?;
    }
    if let Some(col) = output.column_mut(1) {
        *col = Vector::try_from_strings(&database_name_refs, output_allocator.clone())?;
    }
    if let Some(col) = output.column_mut(2) {
        *col = Vector::try_from_strings(&recovery_mode_refs, output_allocator.clone())?;
    }
    for (index, values) in [
        (3usize, &checkpoint_success),
        (4usize, &checkpoint_failure),
        (5usize, &wal_health_checks),
        (6usize, &wal_keep_from),
        (10usize, &queue_depth),
        (11usize, &queue_depth_peak),
        (12usize, &active_workers),
        (13usize, &active_workers_peak),
        (14usize, &mailbox_count),
        (15usize, &applied_lag),
        (16usize, &published_lag),
        (17usize, &durable_wait_count),
        (18usize, &durable_wait_micros),
        (19usize, &applied_wait_count),
        (20usize, &applied_wait_micros),
        (21usize, &published_wait_count),
        (22usize, &published_wait_micros),
        (23usize, &commit_bytes_total),
        (24usize, &group_count),
        (25usize, &group_size_last),
        (26usize, &group_size_peak),
        (27usize, &sync_latency_total),
        (28usize, &sync_latency_peak),
        (29usize, &replay_rowsets_total),
        (30usize, &replay_delete_patches_total),
    ] {
        if let Some(col) = output.column_mut(index) {
            *col = Vector::try_from_i64(values, output_allocator.clone())?;
        }
    }
    for (index, values) in [
        (7usize, &main_truncate),
        (8usize, &checkpoint_truncate),
        (9usize, &recovery_truncate),
    ] {
        if let Some(col) = output.column_mut(index) {
            *col = Vector::try_from_bool(values, output_allocator.clone())?;
        }
    }
    if let Some(col) = output.column_mut(31) {
        *col = Vector::try_from_f64(&inline_patch_ratio, output_allocator.clone())?;
    }

    output.set_cardinality(batch_size);
    if gstate.offset.load(Ordering::Relaxed) >= gstate.entries.len() {
        Ok(TableFunctionResult::Finished)
    } else {
        Ok(TableFunctionResult::HaveMoreOutput)
    }
}

fn paro_wal_metrics_progress(
    _bind_data: Option<&dyn TableFunctionBindData>,
    global_state: Option<&dyn GlobalTableFunctionState>,
) -> f64 {
    global_state
        .map(|state| state.get_progress())
        .unwrap_or(-1.0)
}

pub fn create_paro_wal_metrics_function_set() -> TableFunctionSet {
    let mut func = TableFunction::new("paro_wal_metrics", vec![]);
    func.bind = Some(paro_wal_metrics_bind);
    func.init_global = Some(paro_wal_metrics_init_global);
    func.function = Some(paro_wal_metrics_function);
    func.table_scan_progress = Some(paro_wal_metrics_progress);

    let mut set = TableFunctionSet::new("paro_wal_metrics");
    set.add_function(func);
    set
}

pub fn populate_wal_metric_data(
    state: &mut ParoWalMetricsGlobalState,
    entries: Vec<WalMetricData>,
) {
    state.entries = entries;
    state.offset.store(0, Ordering::Relaxed);
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::HashMap;

    #[test]
    fn test_paro_wal_metrics_bind() {
        let mut return_types = Vec::new();
        let mut names = Vec::new();

        let bind = paro_wal_metrics_bind(
            &TableFunctionBindInput::new(&[], &HashMap::new()),
            &mut return_types,
            &mut names,
        )
        .unwrap();

        assert!(bind.is_some());
        assert_eq!(names[0], "database_oid");
        assert_eq!(names[1], "database_name");
        assert_eq!(names[2], "recovery_mode");
        assert_eq!(return_types.len(), 32);
    }

    #[test]
    fn test_paro_wal_metrics_function_with_data() {
        let input = TableFunctionInitInput::new(None, &[]);
        let mut state_box = paro_wal_metrics_init_global(&input).unwrap().unwrap();
        let state = state_box
            .as_any_mut()
            .downcast_mut::<ParoWalMetricsGlobalState>()
            .unwrap();
        populate_wal_metric_data(
            state,
            vec![WalMetricData {
                database_oid: 42,
                database_name: "postgres".to_string(),
                recovery_mode: "main_and_checkpoint_wal".to_string(),
                checkpoint_success_total: 1,
                checkpoint_failure_total: 0,
                wal_health_check_total: 2,
                wal_keep_from: 99,
                main_wal_needs_truncation: false,
                checkpoint_wal_needs_truncation: true,
                recovery_wal_needs_truncation: false,
                journal_apply_queue_depth: 3,
                journal_apply_queue_depth_peak: 4,
                journal_apply_active_workers: 5,
                journal_apply_active_workers_peak: 6,
                journal_apply_mailbox_count: 7,
                journal_apply_applied_lag: 8,
                journal_apply_published_lag: 9,
                journal_apply_durable_wait_count: 10,
                journal_apply_durable_wait_micros: 11,
                journal_apply_applied_wait_count: 12,
                journal_apply_applied_wait_micros: 13,
                journal_apply_published_wait_count: 14,
                journal_apply_published_wait_micros: 15,
                journal_commit_bytes_total: 16,
                journal_group_count: 17,
                journal_group_size_last: 18,
                journal_group_size_peak: 19,
                journal_sync_latency_micros_total: 20,
                journal_sync_latency_micros_peak: 21,
                journal_replay_rowsets_total: 22,
                journal_replay_delete_patches_total: 23,
                journal_inline_patch_ratio: 0.5,
            }],
        );

        let state_ref = state_box
            .as_any()
            .downcast_ref::<ParoWalMetricsGlobalState>()
            .unwrap();
        let mut input = TableFunctionInput {
            bind_data: None,
            local_state: None,
            global_state: Some(state_ref),
        };
        let mut chunk = paro_common::test_utils::test_chunk_with_capacity(
            &[
                LogicalType::BigInt,
                LogicalType::Varchar,
                LogicalType::Varchar,
                LogicalType::BigInt,
                LogicalType::BigInt,
                LogicalType::BigInt,
                LogicalType::BigInt,
                LogicalType::Boolean,
                LogicalType::Boolean,
                LogicalType::Boolean,
                LogicalType::BigInt,
                LogicalType::BigInt,
                LogicalType::BigInt,
                LogicalType::BigInt,
                LogicalType::BigInt,
                LogicalType::BigInt,
                LogicalType::BigInt,
                LogicalType::BigInt,
                LogicalType::BigInt,
                LogicalType::BigInt,
                LogicalType::BigInt,
                LogicalType::BigInt,
                LogicalType::BigInt,
                LogicalType::BigInt,
                LogicalType::BigInt,
                LogicalType::BigInt,
                LogicalType::BigInt,
                LogicalType::BigInt,
                LogicalType::BigInt,
                LogicalType::BigInt,
                LogicalType::BigInt,
                LogicalType::Double,
            ],
            2048,
        );

        let result = paro_wal_metrics_function(&mut input, &mut chunk).unwrap();
        assert_eq!(result, TableFunctionResult::Finished);
        assert_eq!(chunk.size(), 1);
        assert_eq!(chunk.column(0).unwrap().get_i64(0), Some(42));
        assert_eq!(chunk.column(1).unwrap().get_string(0), Some("postgres"));
        assert_eq!(
            chunk.column(2).unwrap().get_string(0),
            Some("main_and_checkpoint_wal")
        );
        assert_eq!(chunk.column(7).unwrap().get_bool(0), Some(false));
        assert_eq!(chunk.column(8).unwrap().get_bool(0), Some(true));
        assert_eq!(chunk.column(12).unwrap().get_i64(0), Some(5));
        assert_eq!(chunk.column(14).unwrap().get_i64(0), Some(7));
        assert_eq!(chunk.column(22).unwrap().get_i64(0), Some(15));
        assert_eq!(chunk.column(23).unwrap().get_i64(0), Some(16));
        assert_eq!(chunk.column(24).unwrap().get_i64(0), Some(17));
        assert_eq!(chunk.column(25).unwrap().get_i64(0), Some(18));
        assert_eq!(chunk.column(26).unwrap().get_i64(0), Some(19));
        assert_eq!(chunk.column(27).unwrap().get_i64(0), Some(20));
        assert_eq!(chunk.column(28).unwrap().get_i64(0), Some(21));
        assert_eq!(chunk.column(29).unwrap().get_i64(0), Some(22));
        assert_eq!(chunk.column(30).unwrap().get_i64(0), Some(23));
        assert_eq!(chunk.column(31).unwrap().get_f64(0), Some(0.5));
    }
}
