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
pub struct ParoPgSettingsBindData;

impl TableFunctionBindData for ParoPgSettingsBindData {
    fn clone_box(&self) -> Box<dyn TableFunctionBindData> {
        Box::new(self.clone())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[derive(Debug, Clone, Default)]
pub struct SettingRowData {
    pub name: String,
    pub setting: String,
    pub unit: Option<String>,
    pub category: String,
    pub short_desc: Option<String>,
    pub source: String,
    pub vartype: String,
    pub context: String,
}

pub struct ParoPgSettingsGlobalState {
    pub rows: Vec<SettingRowData>,
    pub offset: AtomicUsize,
}

impl GlobalTableFunctionState for ParoPgSettingsGlobalState {
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
        ("setting", LogicalType::Varchar),
        ("unit", LogicalType::Varchar),
        ("category", LogicalType::Varchar),
        ("short_desc", LogicalType::Varchar),
        ("source", LogicalType::Varchar),
        ("vartype", LogicalType::Varchar),
        ("context", LogicalType::Varchar),
    ];

    for (name, ty) in columns {
        names.push(name.to_string());
        return_types.push(ty);
    }

    Ok(Some(Box::new(ParoPgSettingsBindData)))
}

fn init_global(
    _input: &TableFunctionInitInput,
) -> Result<Option<Box<dyn GlobalTableFunctionState>>> {
    Ok(Some(Box::new(ParoPgSettingsGlobalState {
        rows: Vec::new(),
        offset: AtomicUsize::new(0),
    })))
}

fn function(input: &mut TableFunctionInput, output: &mut Chunk) -> Result<TableFunctionResult> {
    let Some(state) = input
        .global_state
        .and_then(|gs| gs.as_any().downcast_ref::<ParoPgSettingsGlobalState>())
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
    let settings: Vec<&str> = slice.iter().map(|row| row.setting.as_str()).collect();
    let units: Vec<Option<&str>> = slice.iter().map(|row| row.unit.as_deref()).collect();
    let categories: Vec<&str> = slice.iter().map(|row| row.category.as_str()).collect();
    let short_descs: Vec<Option<&str>> =
        slice.iter().map(|row| row.short_desc.as_deref()).collect();
    let sources: Vec<&str> = slice.iter().map(|row| row.source.as_str()).collect();
    let vartypes: Vec<&str> = slice.iter().map(|row| row.vartype.as_str()).collect();
    let contexts: Vec<&str> = slice.iter().map(|row| row.context.as_str()).collect();

    if let Some(col) = output.column_mut(0) {
        *col = Vector::from_strings(&names);
    }
    if let Some(col) = output.column_mut(1) {
        *col = Vector::from_strings(&settings);
    }
    if let Some(col) = output.column_mut(2) {
        *col = Vector::from_nullable_strings(&units);
    }
    if let Some(col) = output.column_mut(3) {
        *col = Vector::from_strings(&categories);
    }
    if let Some(col) = output.column_mut(4) {
        *col = Vector::from_nullable_strings(&short_descs);
    }
    if let Some(col) = output.column_mut(5) {
        *col = Vector::from_strings(&sources);
    }
    if let Some(col) = output.column_mut(6) {
        *col = Vector::from_strings(&vartypes);
    }
    if let Some(col) = output.column_mut(7) {
        *col = Vector::from_strings(&contexts);
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

pub fn create_paro_pg_settings_function_set() -> TableFunctionSet {
    let mut func = TableFunction::new("paro_pg_settings", vec![]);
    func.bind = Some(bind);
    func.init_global = Some(init_global);
    func.function = Some(function);
    func.table_scan_progress = Some(progress);

    let mut set = TableFunctionSet::new("paro_pg_settings");
    set.add_function(func);
    set
}

pub fn populate_settings_data(state: &mut ParoPgSettingsGlobalState, rows: Vec<SettingRowData>) {
    state.rows = rows;
    state.offset.store(0, Ordering::Relaxed);
}
