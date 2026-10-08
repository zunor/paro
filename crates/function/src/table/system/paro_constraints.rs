// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Catalog-backed table constraint introspection.

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
struct ParoConstraintsBindData;

impl TableFunctionBindData for ParoConstraintsBindData {
    fn clone_box(&self) -> Box<dyn TableFunctionBindData> {
        Box::new(self.clone())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// One catalog constraint column. Constraints without key columns have one row
/// with a NULL `column_name` and `ordinal_position`.
#[derive(Debug, Clone)]
pub struct ConstraintData {
    pub database_name: String,
    pub schema_name: String,
    pub table_name: String,
    pub constraint_name: String,
    pub constraint_type: String,
    pub enforced: bool,
    pub column_name: Option<String>,
    pub ordinal_position: Option<i64>,
}

pub struct ParoConstraintsGlobalState {
    pub entries: Vec<ConstraintData>,
    offset: AtomicUsize,
}

impl GlobalTableFunctionState for ParoConstraintsGlobalState {
    fn max_threads(&self) -> usize {
        1
    }

    fn get_progress(&self) -> f64 {
        if self.entries.is_empty() {
            return 100.0;
        }
        100.0 * self.offset.load(Ordering::Relaxed) as f64 / self.entries.len() as f64
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
    names.extend([
        "database_name".to_string(),
        "schema_name".to_string(),
        "table_name".to_string(),
        "constraint_name".to_string(),
        "constraint_type".to_string(),
        "enforced".to_string(),
        "column_name".to_string(),
        "ordinal_position".to_string(),
    ]);
    return_types.extend([
        LogicalType::Varchar,
        LogicalType::Varchar,
        LogicalType::Varchar,
        LogicalType::Varchar,
        LogicalType::Varchar,
        LogicalType::Boolean,
        LogicalType::Varchar,
        LogicalType::BigInt,
    ]);
    Ok(Some(Box::new(ParoConstraintsBindData)))
}

fn init_global(
    _input: &TableFunctionInitInput,
) -> Result<Option<Box<dyn GlobalTableFunctionState>>> {
    Ok(Some(Box::new(ParoConstraintsGlobalState {
        entries: Vec::new(),
        offset: AtomicUsize::new(0),
    })))
}

fn scan(input: &mut TableFunctionInput, output: &mut Chunk) -> Result<TableFunctionResult> {
    let Some(state) = input
        .global_state
        .and_then(|state| state.as_any().downcast_ref::<ParoConstraintsGlobalState>())
    else {
        output.set_cardinality(0);
        return Ok(TableFunctionResult::Finished);
    };
    let offset = state.offset.load(Ordering::Relaxed);
    if offset >= state.entries.len() {
        output.set_cardinality(0);
        return Ok(TableFunctionResult::Finished);
    }

    let entries = &state.entries[offset..(offset + 2048).min(state.entries.len())];
    let allocator = output.allocator().clone();
    let strings = |values: Vec<&str>| Vector::try_from_strings(&values, allocator.clone());
    let optional_strings =
        |values: Vec<Option<&str>>| Vector::try_from_nullable_strings(&values, allocator.clone());

    *output.column_mut(0).expect("database_name output") = strings(
        entries
            .iter()
            .map(|entry| entry.database_name.as_str())
            .collect(),
    )?;
    *output.column_mut(1).expect("schema_name output") = strings(
        entries
            .iter()
            .map(|entry| entry.schema_name.as_str())
            .collect(),
    )?;
    *output.column_mut(2).expect("table_name output") = strings(
        entries
            .iter()
            .map(|entry| entry.table_name.as_str())
            .collect(),
    )?;
    *output.column_mut(3).expect("constraint_name output") = strings(
        entries
            .iter()
            .map(|entry| entry.constraint_name.as_str())
            .collect(),
    )?;
    *output.column_mut(4).expect("constraint_type output") = strings(
        entries
            .iter()
            .map(|entry| entry.constraint_type.as_str())
            .collect(),
    )?;
    *output.column_mut(5).expect("enforced output") = Vector::try_from_bool(
        &entries
            .iter()
            .map(|entry| entry.enforced)
            .collect::<Vec<_>>(),
        allocator.clone(),
    )?;
    *output.column_mut(6).expect("column_name output") = optional_strings(
        entries
            .iter()
            .map(|entry| entry.column_name.as_deref())
            .collect(),
    )?;
    *output.column_mut(7).expect("ordinal_position output") = Vector::try_from_nullable_u64(
        &entries
            .iter()
            .map(|entry| entry.ordinal_position.map(|position| position as u64))
            .collect::<Vec<_>>(),
        allocator,
    )?;

    state.offset.fetch_add(entries.len(), Ordering::Relaxed);
    output.set_cardinality(entries.len());
    if offset + entries.len() == state.entries.len() {
        Ok(TableFunctionResult::Finished)
    } else {
        Ok(TableFunctionResult::HaveMoreOutput)
    }
}

pub fn create_paro_constraints_function_set() -> TableFunctionSet {
    let mut function = TableFunction::new("paro_constraints", vec![]);
    function.bind = Some(bind);
    function.init_global = Some(init_global);
    function.function = Some(scan);
    function.table_scan_progress =
        Some(|_, state| state.map_or(-1.0, |state| state.get_progress()));
    let mut set = TableFunctionSet::new("paro_constraints");
    set.add_function(function);
    set
}

pub fn populate_constraint_data(
    state: &mut ParoConstraintsGlobalState,
    entries: Vec<ConstraintData>,
) {
    state.entries = entries;
    state.offset.store(0, Ordering::Relaxed);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exposes_constraint_and_key_column_contract() {
        let named_parameters = std::collections::HashMap::new();
        let input = TableFunctionBindInput::new(&[], &named_parameters);
        let mut types = Vec::new();
        let mut names = Vec::new();
        bind(&input, &mut types, &mut names).unwrap();
        assert_eq!(names.len(), 8);
        assert_eq!(names[3], "constraint_name");
        assert_eq!(names[6], "column_name");
        assert_eq!(types[7], LogicalType::BigInt);
    }
}
