// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

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

use super::memory_runtime::get_system_buffer_manager;

#[derive(Clone)]
struct PragmaDatabaseSizeBindData;

impl TableFunctionBindData for PragmaDatabaseSizeBindData {
    fn clone_box(&self) -> Box<dyn TableFunctionBindData> {
        Box::new(self.clone())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[derive(Debug, Clone)]
struct DatabaseSizeRow {
    database_name: String,
    database_size: i64,
    block_size: i64,
    memory_usage: i64,
    memory_limit: i64,
}

struct PragmaDatabaseSizeGlobalState {
    rows: Vec<DatabaseSizeRow>,
    offset: AtomicUsize,
}

impl GlobalTableFunctionState for PragmaDatabaseSizeGlobalState {
    fn max_threads(&self) -> usize {
        1
    }

    fn get_progress(&self) -> f64 {
        if self.rows.is_empty() {
            return 100.0;
        }
        let offset = self.offset.load(Ordering::Relaxed);
        (offset as f64 / self.rows.len() as f64) * 100.0
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

fn pragma_database_size_bind(
    _input: &TableFunctionBindInput,
    return_types: &mut Vec<LogicalType>,
    names: &mut Vec<String>,
) -> Result<Option<Box<dyn TableFunctionBindData>>> {
    names.push("database_name".to_string());
    return_types.push(LogicalType::Varchar);

    names.push("database_size".to_string());
    return_types.push(LogicalType::BigInt);

    names.push("block_size".to_string());
    return_types.push(LogicalType::BigInt);

    names.push("memory_usage".to_string());
    return_types.push(LogicalType::BigInt);

    names.push("memory_limit".to_string());
    return_types.push(LogicalType::BigInt);

    Ok(Some(Box::new(PragmaDatabaseSizeBindData)))
}

fn pragma_database_size_init_global(
    _input: &TableFunctionInitInput,
) -> Result<Option<Box<dyn GlobalTableFunctionState>>> {
    let rows = get_system_buffer_manager()
        .map(|buffer_manager| {
            let used_memory = i64::try_from(buffer_manager.get_used_memory()).unwrap_or(i64::MAX);
            let memory_limit = i64::try_from(buffer_manager.get_max_memory()).unwrap_or(i64::MAX);
            let block_size = i64::try_from(buffer_manager.get_block_size()).unwrap_or(i64::MAX);
            let temporary_file_bytes: u64 = buffer_manager
                .get_temporary_files()
                .into_iter()
                .map(|file| file.size)
                .sum();
            let temporary_file_bytes = i64::try_from(temporary_file_bytes).unwrap_or(i64::MAX);

            vec![DatabaseSizeRow {
                database_name: "main".to_string(),
                database_size: used_memory.saturating_add(temporary_file_bytes),
                block_size,
                memory_usage: used_memory,
                memory_limit,
            }]
        })
        .unwrap_or_default();

    Ok(Some(Box::new(PragmaDatabaseSizeGlobalState {
        rows,
        offset: AtomicUsize::new(0),
    })))
}

fn pragma_database_size_function(
    input: &mut TableFunctionInput,
    output: &mut Chunk,
) -> Result<TableFunctionResult> {
    let Some(gstate) = input.global_state.and_then(|state| {
        state
            .as_any()
            .downcast_ref::<PragmaDatabaseSizeGlobalState>()
    }) else {
        output.set_cardinality(0);
        return Ok(TableFunctionResult::Finished);
    };

    let offset = gstate.offset.load(Ordering::Relaxed);
    if offset >= gstate.rows.len() {
        output.set_cardinality(0);
        return Ok(TableFunctionResult::Finished);
    }

    let batch_size = 2048.min(gstate.rows.len() - offset);
    let rows = &gstate.rows[offset..offset + batch_size];
    gstate.offset.fetch_add(batch_size, Ordering::Relaxed);

    let database_names: Vec<&str> = rows.iter().map(|row| row.database_name.as_str()).collect();
    let database_sizes: Vec<i64> = rows.iter().map(|row| row.database_size).collect();
    let block_sizes: Vec<i64> = rows.iter().map(|row| row.block_size).collect();
    let memory_usages: Vec<i64> = rows.iter().map(|row| row.memory_usage).collect();
    let memory_limits: Vec<i64> = rows.iter().map(|row| row.memory_limit).collect();

    if let Some(col) = output.column_mut(0) {
        *col = Vector::from_strings(&database_names);
    }
    if let Some(col) = output.column_mut(1) {
        *col = Vector::from_i64(&database_sizes);
    }
    if let Some(col) = output.column_mut(2) {
        *col = Vector::from_i64(&block_sizes);
    }
    if let Some(col) = output.column_mut(3) {
        *col = Vector::from_i64(&memory_usages);
    }
    if let Some(col) = output.column_mut(4) {
        *col = Vector::from_i64(&memory_limits);
    }
    output.set_cardinality(batch_size);

    if gstate.offset.load(Ordering::Relaxed) >= gstate.rows.len() {
        Ok(TableFunctionResult::Finished)
    } else {
        Ok(TableFunctionResult::HaveMoreOutput)
    }
}

fn pragma_database_size_progress(
    _bind_data: Option<&dyn TableFunctionBindData>,
    global_state: Option<&dyn GlobalTableFunctionState>,
) -> f64 {
    global_state.map_or(-1.0, |state| state.get_progress())
}

pub fn create_pragma_database_size_function_set() -> TableFunctionSet {
    let mut func = TableFunction::new("pragma_database_size", vec![]);
    func.bind = Some(pragma_database_size_bind);
    func.init_global = Some(pragma_database_size_init_global);
    func.function = Some(pragma_database_size_function);
    func.table_scan_progress = Some(pragma_database_size_progress);

    let mut set = TableFunctionSet::new("pragma_database_size");
    set.add_function(func);
    set
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_pragma_database_size_function_set() {
        let set = create_pragma_database_size_function_set();
        assert_eq!(set.name, "pragma_database_size");
        assert_eq!(set.functions.len(), 1);
    }
}
