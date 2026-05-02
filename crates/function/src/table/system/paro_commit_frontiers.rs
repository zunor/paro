// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! paro_commit_frontiers() table function.

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
pub struct ParoCommitFrontiersBindData;

impl TableFunctionBindData for ParoCommitFrontiersBindData {
    fn clone_box(&self) -> Box<dyn TableFunctionBindData> {
        Box::new(self.clone())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[derive(Debug, Clone)]
pub struct CommitFrontierData {
    pub database_oid: u64,
    pub database_name: String,
    pub durable_commit_id: u64,
    pub published_commit_id: u64,
    pub durable_commit_bytes: u64,
    pub published_commit_bytes: u64,
    pub durable_to_published_bytes_lag: Option<u64>,
    pub stale_bytes_at_poison: Option<u64>,
    pub publish_failure_watermark: Option<u64>,
    pub publish_failure_cause: Option<String>,
    pub wait_count: u64,
    pub wait_wake_count: u64,
    pub notify_all_count: u64,
    pub notify_suppressed_count: u64,
    pub publish_failure_count: u64,
}

pub struct ParoCommitFrontiersGlobalState {
    pub entries: Vec<CommitFrontierData>,
    pub offset: AtomicUsize,
}

impl GlobalTableFunctionState for ParoCommitFrontiersGlobalState {
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

fn bind(
    _input: &TableFunctionBindInput,
    return_types: &mut Vec<LogicalType>,
    names: &mut Vec<String>,
) -> Result<Option<Box<dyn TableFunctionBindData>>> {
    for (name, ty) in [
        ("database_oid", LogicalType::BigInt),
        ("database_name", LogicalType::Varchar),
        ("durable_commit_id", LogicalType::BigInt),
        ("published_commit_id", LogicalType::BigInt),
        ("durable_commit_bytes", LogicalType::BigInt),
        ("published_commit_bytes", LogicalType::BigInt),
        ("durable_to_published_bytes_lag", LogicalType::BigInt),
        ("stale_bytes_at_poison", LogicalType::BigInt),
        ("publish_failure_watermark", LogicalType::BigInt),
        ("publish_failure_cause", LogicalType::Varchar),
        ("wait_count", LogicalType::BigInt),
        ("wait_wake_count", LogicalType::BigInt),
        ("notify_all_count", LogicalType::BigInt),
        ("notify_suppressed_count", LogicalType::BigInt),
        ("publish_failure_count", LogicalType::BigInt),
    ] {
        names.push(name.to_string());
        return_types.push(ty);
    }
    Ok(Some(Box::new(ParoCommitFrontiersBindData)))
}

fn init_global(
    _input: &TableFunctionInitInput,
) -> Result<Option<Box<dyn GlobalTableFunctionState>>> {
    Ok(Some(Box::new(ParoCommitFrontiersGlobalState {
        entries: Vec::new(),
        offset: AtomicUsize::new(0),
    })))
}

fn function(input: &mut TableFunctionInput, output: &mut Chunk) -> Result<TableFunctionResult> {
    let allocator = output.allocator().clone();
    let Some(gstate) = input.global_state.and_then(|state| {
        state
            .as_any()
            .downcast_ref::<ParoCommitFrontiersGlobalState>()
    }) else {
        output.set_cardinality(0);
        return Ok(TableFunctionResult::Finished);
    };

    let offset = gstate.offset.load(Ordering::Relaxed);
    if offset >= gstate.entries.len() {
        output.set_cardinality(0);
        return Ok(TableFunctionResult::Finished);
    }

    let batch_size = 2048.min(gstate.entries.len() - offset);
    let rows = gstate
        .entries
        .iter()
        .skip(offset)
        .take(batch_size)
        .collect::<Vec<_>>();
    gstate.offset.fetch_add(batch_size, Ordering::Relaxed);

    let database_oids = rows
        .iter()
        .map(|entry| u64_to_i64(entry.database_oid))
        .collect::<Vec<_>>();
    let database_names = rows
        .iter()
        .map(|entry| entry.database_name.as_str())
        .collect::<Vec<_>>();
    let failure_causes = rows
        .iter()
        .map(|entry| entry.publish_failure_cause.as_deref())
        .collect::<Vec<_>>();
    let mut non_nullable: [Vec<i64>; 9] = std::array::from_fn(|_| Vec::with_capacity(batch_size));
    let mut nullable: [Vec<Option<u64>>; 3] =
        std::array::from_fn(|_| Vec::with_capacity(batch_size));

    for entry in rows {
        for (slot, value) in [
            entry.durable_commit_id,
            entry.published_commit_id,
            entry.durable_commit_bytes,
            entry.published_commit_bytes,
            entry.wait_count,
            entry.wait_wake_count,
            entry.notify_all_count,
            entry.notify_suppressed_count,
            entry.publish_failure_count,
        ]
        .into_iter()
        .enumerate()
        {
            non_nullable[slot].push(u64_to_i64(value));
        }
        for (slot, value) in [
            entry.durable_to_published_bytes_lag,
            entry.stale_bytes_at_poison,
            entry.publish_failure_watermark,
        ]
        .into_iter()
        .enumerate()
        {
            nullable[slot].push(value);
        }
    }

    if let Some(col) = output.column_mut(0) {
        *col = Vector::try_from_i64(&database_oids, allocator.clone())?;
    }
    if let Some(col) = output.column_mut(1) {
        *col = Vector::try_from_strings(&database_names, allocator.clone())?;
    }
    for (slot, values) in non_nullable.iter().take(4).enumerate() {
        if let Some(col) = output.column_mut(slot + 2) {
            *col = Vector::try_from_i64(values, allocator.clone())?;
        }
    }
    for (slot, values) in nullable.iter().enumerate() {
        if let Some(col) = output.column_mut(slot + 6) {
            *col = Vector::try_from_nullable_u64(values, allocator.clone())?;
        }
    }
    if let Some(col) = output.column_mut(9) {
        *col = Vector::try_from_nullable_strings(&failure_causes, allocator.clone())?;
    }
    for (slot, values) in non_nullable.iter().skip(4).enumerate() {
        if let Some(col) = output.column_mut(slot + 10) {
            *col = Vector::try_from_i64(values, allocator.clone())?;
        }
    }

    output.set_cardinality(batch_size);
    if gstate.offset.load(Ordering::Relaxed) >= gstate.entries.len() {
        Ok(TableFunctionResult::Finished)
    } else {
        Ok(TableFunctionResult::HaveMoreOutput)
    }
}

fn progress(
    _bind_data: Option<&dyn TableFunctionBindData>,
    global_state: Option<&dyn GlobalTableFunctionState>,
) -> f64 {
    global_state
        .map(|state| state.get_progress())
        .unwrap_or(-1.0)
}

pub fn create_paro_commit_frontiers_function_set() -> TableFunctionSet {
    let mut func = TableFunction::new("paro_commit_frontiers", vec![]);
    func.bind = Some(bind);
    func.init_global = Some(init_global);
    func.function = Some(function);
    func.table_scan_progress = Some(progress);

    let mut set = TableFunctionSet::new("paro_commit_frontiers");
    set.add_function(func);
    set
}

pub fn populate_commit_frontier_data(
    state: &mut ParoCommitFrontiersGlobalState,
    entries: Vec<CommitFrontierData>,
) {
    state.entries = entries;
    state.offset.store(0, Ordering::Relaxed);
}

fn u64_to_i64(value: u64) -> i64 {
    value.min(i64::MAX as u64) as i64
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn commit_frontiers_bind_exposes_debug_columns() {
        let mut return_types = Vec::new();
        let mut names = Vec::new();

        let bind = bind(
            &TableFunctionBindInput::new(&[], &HashMap::new()),
            &mut return_types,
            &mut names,
        )
        .unwrap();

        assert!(bind.is_some());
        assert_eq!(names[0], "database_oid");
        assert_eq!(names[2], "durable_commit_id");
        assert_eq!(names[8], "publish_failure_watermark");
        assert_eq!(names[14], "publish_failure_count");
        assert_eq!(return_types.len(), 15);
    }
}
