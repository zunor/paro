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
struct ParoTemporaryFilesBindData;

impl TableFunctionBindData for ParoTemporaryFilesBindData {
    fn clone_box(&self) -> Box<dyn TableFunctionBindData> {
        Box::new(self.clone())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[derive(Debug, Clone)]
struct TemporaryFileRow {
    path: String,
    size: i64,
    write_bytes: i64,
    read_bytes: i64,
    file_count: i64,
    swap_usage: i64,
    swap_limit_hits: i64,
}

struct ParoTemporaryFilesGlobalState {
    rows: Vec<TemporaryFileRow>,
    offset: AtomicUsize,
}

impl GlobalTableFunctionState for ParoTemporaryFilesGlobalState {
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

fn paro_temporary_files_bind(
    _input: &TableFunctionBindInput,
    return_types: &mut Vec<LogicalType>,
    names: &mut Vec<String>,
) -> Result<Option<Box<dyn TableFunctionBindData>>> {
    names.push("path".to_string());
    return_types.push(LogicalType::Varchar);

    names.push("size".to_string());
    return_types.push(LogicalType::BigInt);

    names.push("write_bytes".to_string());
    return_types.push(LogicalType::BigInt);

    names.push("read_bytes".to_string());
    return_types.push(LogicalType::BigInt);

    names.push("file_count".to_string());
    return_types.push(LogicalType::BigInt);

    names.push("swap_usage".to_string());
    return_types.push(LogicalType::BigInt);

    names.push("swap_limit_hits".to_string());
    return_types.push(LogicalType::BigInt);

    Ok(Some(Box::new(ParoTemporaryFilesBindData)))
}

fn paro_temporary_files_init_global(
    _input: &TableFunctionInitInput,
) -> Result<Option<Box<dyn GlobalTableFunctionState>>> {
    let rows = get_system_buffer_manager()
        .map(|buffer_manager| {
            let files = buffer_manager.get_temporary_files();
            let metrics = buffer_manager.get_temporary_spill_metrics();

            if files.is_empty() {
                return vec![TemporaryFileRow {
                    path: String::new(),
                    size: 0,
                    write_bytes: i64::try_from(metrics.write_bytes).unwrap_or(i64::MAX),
                    read_bytes: i64::try_from(metrics.read_bytes).unwrap_or(i64::MAX),
                    file_count: i64::try_from(metrics.file_count).unwrap_or(i64::MAX),
                    swap_usage: i64::try_from(metrics.swap_usage).unwrap_or(i64::MAX),
                    swap_limit_hits: i64::try_from(metrics.swap_limit_hits).unwrap_or(i64::MAX),
                }];
            }

            files
                .into_iter()
                .map(|file| TemporaryFileRow {
                    path: file.path.display().to_string(),
                    size: i64::try_from(file.size).unwrap_or(i64::MAX),
                    write_bytes: i64::try_from(metrics.write_bytes).unwrap_or(i64::MAX),
                    read_bytes: i64::try_from(metrics.read_bytes).unwrap_or(i64::MAX),
                    file_count: i64::try_from(metrics.file_count).unwrap_or(i64::MAX),
                    swap_usage: i64::try_from(metrics.swap_usage).unwrap_or(i64::MAX),
                    swap_limit_hits: i64::try_from(metrics.swap_limit_hits).unwrap_or(i64::MAX),
                })
                .collect()
        })
        .unwrap_or_default();

    Ok(Some(Box::new(ParoTemporaryFilesGlobalState {
        rows,
        offset: AtomicUsize::new(0),
    })))
}

fn paro_temporary_files_function(
    input: &mut TableFunctionInput,
    output: &mut Chunk,
) -> Result<TableFunctionResult> {
    let Some(gstate) = input.global_state.and_then(|state| {
        state
            .as_any()
            .downcast_ref::<ParoTemporaryFilesGlobalState>()
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

    let paths: Vec<&str> = rows.iter().map(|row| row.path.as_str()).collect();
    let sizes: Vec<i64> = rows.iter().map(|row| row.size).collect();
    let write_bytes: Vec<i64> = rows.iter().map(|row| row.write_bytes).collect();
    let read_bytes: Vec<i64> = rows.iter().map(|row| row.read_bytes).collect();
    let file_count: Vec<i64> = rows.iter().map(|row| row.file_count).collect();
    let swap_usage: Vec<i64> = rows.iter().map(|row| row.swap_usage).collect();
    let swap_limit_hits: Vec<i64> = rows.iter().map(|row| row.swap_limit_hits).collect();

    if let Some(col) = output.column_mut(0) {
        *col = Vector::from_strings(&paths);
    }
    if let Some(col) = output.column_mut(1) {
        *col = Vector::from_i64(&sizes);
    }
    if let Some(col) = output.column_mut(2) {
        *col = Vector::from_i64(&write_bytes);
    }
    if let Some(col) = output.column_mut(3) {
        *col = Vector::from_i64(&read_bytes);
    }
    if let Some(col) = output.column_mut(4) {
        *col = Vector::from_i64(&file_count);
    }
    if let Some(col) = output.column_mut(5) {
        *col = Vector::from_i64(&swap_usage);
    }
    if let Some(col) = output.column_mut(6) {
        *col = Vector::from_i64(&swap_limit_hits);
    }
    output.set_cardinality(batch_size);

    if gstate.offset.load(Ordering::Relaxed) >= gstate.rows.len() {
        Ok(TableFunctionResult::Finished)
    } else {
        Ok(TableFunctionResult::HaveMoreOutput)
    }
}

fn paro_temporary_files_progress(
    _bind_data: Option<&dyn TableFunctionBindData>,
    global_state: Option<&dyn GlobalTableFunctionState>,
) -> f64 {
    global_state.map_or(-1.0, |state| state.get_progress())
}

pub fn create_paro_temporary_files_function_set() -> TableFunctionSet {
    let mut func = TableFunction::new("paro_temporary_files", vec![]);
    func.bind = Some(paro_temporary_files_bind);
    func.init_global = Some(paro_temporary_files_init_global);
    func.function = Some(paro_temporary_files_function);
    func.table_scan_progress = Some(paro_temporary_files_progress);

    let mut set = TableFunctionSet::new("paro_temporary_files");
    set.add_function(func);
    set
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_paro_temporary_files_function_set() {
        let set = create_paro_temporary_files_function_set();
        assert_eq!(set.name, "paro_temporary_files");
        assert_eq!(set.functions.len(), 1);
    }
}
