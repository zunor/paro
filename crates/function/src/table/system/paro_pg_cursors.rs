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

#[derive(Clone)]
pub struct ParoPgCursorsBindData;

impl TableFunctionBindData for ParoPgCursorsBindData {
    fn clone_box(&self) -> Box<dyn TableFunctionBindData> {
        Box::new(self.clone())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[derive(Debug, Clone, Default)]
pub struct CursorSummaryData {
    pub name: String,
    pub statement: String,
    pub is_holdable: bool,
    pub is_binary: bool,
    pub is_scrollable: bool,
}

pub struct ParoPgCursorsGlobalState {
    pub rows: Vec<CursorSummaryData>,
    pub offset: AtomicUsize,
}

impl GlobalTableFunctionState for ParoPgCursorsGlobalState {
    fn max_threads(&self) -> usize {
        1
    }

    fn get_progress(&self) -> f64 {
        if self.rows.is_empty() {
            100.0
        } else {
            (self.offset.load(Ordering::Relaxed) as f64 / self.rows.len() as f64) * 100.0
        }
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
    let columns = [
        ("name", LogicalType::Varchar),
        ("statement", LogicalType::Varchar),
        ("is_holdable", LogicalType::Boolean),
        ("is_binary", LogicalType::Boolean),
        ("is_scrollable", LogicalType::Boolean),
    ];

    for (name, ty) in columns {
        names.push(name.to_string());
        return_types.push(ty);
    }

    Ok(Some(Box::new(ParoPgCursorsBindData)))
}

fn init_global(
    _input: &TableFunctionInitInput,
) -> Result<Option<Box<dyn GlobalTableFunctionState>>> {
    Ok(Some(Box::new(ParoPgCursorsGlobalState {
        rows: Vec::new(),
        offset: AtomicUsize::new(0),
    })))
}

fn function(input: &mut TableFunctionInput, output: &mut Chunk) -> Result<TableFunctionResult> {
    let output_allocator = output.allocator().clone();
    let Some(state) = input
        .global_state
        .and_then(|gs| gs.as_any().downcast_ref::<ParoPgCursorsGlobalState>())
    else {
        output.set_cardinality(0);
        return Ok(TableFunctionResult::Finished);
    };

    let offset = state.offset.load(Ordering::Relaxed);
    if offset >= state.rows.len() {
        output.set_cardinality(0);
        return Ok(TableFunctionResult::Finished);
    }

    let batch_size = 2048.min(state.rows.len() - offset);
    let slice = &state.rows[offset..offset + batch_size];

    let names: Vec<&str> = slice.iter().map(|row| row.name.as_str()).collect();
    let statements: Vec<&str> = slice.iter().map(|row| row.statement.as_str()).collect();
    let holdable: Vec<bool> = slice.iter().map(|row| row.is_holdable).collect();
    let binary: Vec<bool> = slice.iter().map(|row| row.is_binary).collect();
    let scrollable: Vec<bool> = slice.iter().map(|row| row.is_scrollable).collect();

    if let Some(col) = output.column_mut(0) {
        *col = Vector::try_from_strings(&names, output_allocator.clone())?;
    }
    if let Some(col) = output.column_mut(1) {
        *col = Vector::try_from_strings(&statements, output_allocator.clone())?;
    }
    if let Some(col) = output.column_mut(2) {
        *col = Vector::try_from_bool(&holdable, output_allocator.clone())?;
    }
    if let Some(col) = output.column_mut(3) {
        *col = Vector::try_from_bool(&binary, output_allocator.clone())?;
    }
    if let Some(col) = output.column_mut(4) {
        *col = Vector::try_from_bool(&scrollable, output_allocator.clone())?;
    }

    output.set_cardinality(batch_size);
    state.offset.fetch_add(batch_size, Ordering::Relaxed);

    if state.offset.load(Ordering::Relaxed) >= state.rows.len() {
        Ok(TableFunctionResult::Finished)
    } else {
        Ok(TableFunctionResult::HaveMoreOutput)
    }
}

fn progress(
    _bind_data: Option<&dyn TableFunctionBindData>,
    global_state: Option<&dyn GlobalTableFunctionState>,
) -> f64 {
    global_state.map(|s| s.get_progress()).unwrap_or(-1.0)
}

pub fn create_paro_pg_cursors_function_set() -> TableFunctionSet {
    let mut func = TableFunction::new("paro_pg_cursors", vec![]);
    func.bind = Some(bind);
    func.init_global = Some(init_global);
    func.function = Some(function);
    func.table_scan_progress = Some(progress);

    let mut set = TableFunctionSet::new("paro_pg_cursors");
    set.add_function(func);
    set
}

pub fn populate_cursor_data(state: &mut ParoPgCursorsGlobalState, rows: Vec<CursorSummaryData>) {
    state.rows = rows;
    state.offset.store(0, Ordering::Relaxed);
}
