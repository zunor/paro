// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! paro_commit_poison() table function.

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
pub struct ParoCommitPoisonBindData;

impl TableFunctionBindData for ParoCommitPoisonBindData {
    fn clone_box(&self) -> Box<dyn TableFunctionBindData> {
        Box::new(self.clone())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[derive(Debug, Clone)]
pub struct CommitPoisonData {
    pub database_oid: u64,
    pub database_name: String,
    pub admission_state: String,
    pub admission_open: bool,
    pub poisoned: bool,
    pub poison_cause: Option<String>,
    pub first_blocked_commit_ts: Option<u64>,
}

pub struct ParoCommitPoisonGlobalState {
    pub entries: Vec<CommitPoisonData>,
    pub offset: AtomicUsize,
}

impl GlobalTableFunctionState for ParoCommitPoisonGlobalState {
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
        ("admission_state", LogicalType::Varchar),
        ("admission_open", LogicalType::Boolean),
        ("poisoned", LogicalType::Boolean),
        ("poison_cause", LogicalType::Varchar),
        ("first_blocked_commit_ts", LogicalType::BigInt),
    ] {
        names.push(name.to_string());
        return_types.push(ty);
    }
    Ok(Some(Box::new(ParoCommitPoisonBindData)))
}

fn init_global(
    _input: &TableFunctionInitInput,
) -> Result<Option<Box<dyn GlobalTableFunctionState>>> {
    Ok(Some(Box::new(ParoCommitPoisonGlobalState {
        entries: Vec::new(),
        offset: AtomicUsize::new(0),
    })))
}

fn function(input: &mut TableFunctionInput, output: &mut Chunk) -> Result<TableFunctionResult> {
    let allocator = output.allocator().clone();
    let Some(gstate) = input
        .global_state
        .and_then(|state| state.as_any().downcast_ref::<ParoCommitPoisonGlobalState>())
    else {
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
        .map(|entry| entry.database_oid.min(i64::MAX as u64) as i64)
        .collect::<Vec<_>>();
    let database_names = rows
        .iter()
        .map(|entry| entry.database_name.as_str())
        .collect::<Vec<_>>();
    let admission_states = rows
        .iter()
        .map(|entry| entry.admission_state.as_str())
        .collect::<Vec<_>>();
    let admission_open = rows
        .iter()
        .map(|entry| entry.admission_open)
        .collect::<Vec<_>>();
    let poisoned = rows.iter().map(|entry| entry.poisoned).collect::<Vec<_>>();
    let poison_causes = rows
        .iter()
        .map(|entry| entry.poison_cause.as_deref())
        .collect::<Vec<_>>();
    let first_blocked = rows
        .iter()
        .map(|entry| entry.first_blocked_commit_ts)
        .collect::<Vec<_>>();

    if let Some(col) = output.column_mut(0) {
        *col = Vector::try_from_i64(&database_oids, allocator.clone())?;
    }
    if let Some(col) = output.column_mut(1) {
        *col = Vector::try_from_strings(&database_names, allocator.clone())?;
    }
    if let Some(col) = output.column_mut(2) {
        *col = Vector::try_from_strings(&admission_states, allocator.clone())?;
    }
    if let Some(col) = output.column_mut(3) {
        *col = Vector::try_from_bool(&admission_open, allocator.clone())?;
    }
    if let Some(col) = output.column_mut(4) {
        *col = Vector::try_from_bool(&poisoned, allocator.clone())?;
    }
    if let Some(col) = output.column_mut(5) {
        *col = Vector::try_from_nullable_strings(&poison_causes, allocator.clone())?;
    }
    if let Some(col) = output.column_mut(6) {
        *col = Vector::try_from_nullable_u64(&first_blocked, allocator.clone())?;
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

pub fn create_paro_commit_poison_function_set() -> TableFunctionSet {
    let mut func = TableFunction::new("paro_commit_poison", vec![]);
    func.bind = Some(bind);
    func.init_global = Some(init_global);
    func.function = Some(function);
    func.table_scan_progress = Some(progress);

    let mut set = TableFunctionSet::new("paro_commit_poison");
    set.add_function(func);
    set
}

pub fn populate_commit_poison_data(
    state: &mut ParoCommitPoisonGlobalState,
    entries: Vec<CommitPoisonData>,
) {
    state.entries = entries;
    state.offset.store(0, Ordering::Relaxed);
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn commit_poison_bind_exposes_admission_and_cause() {
        let mut return_types = Vec::new();
        let mut names = Vec::new();

        let bind = bind(
            &TableFunctionBindInput::new(&[], &HashMap::new()),
            &mut return_types,
            &mut names,
        )
        .unwrap();

        assert!(bind.is_some());
        assert_eq!(
            names,
            vec![
                "database_oid",
                "database_name",
                "admission_state",
                "admission_open",
                "poisoned",
                "poison_cause",
                "first_blocked_commit_ts"
            ]
        );
        assert_eq!(return_types.len(), 7);
    }
}
