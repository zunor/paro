// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! paro_optimizers() Table Function

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
pub struct ParoOptimizersBindData;

impl TableFunctionBindData for ParoOptimizersBindData {
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
pub struct OptimizerData {
    pub name: String,
    pub enabled: bool,
    pub last_elapsed_us: i64,
    pub invocation_count: i64,
}

pub struct ParoOptimizersGlobalState {
    pub entries: Vec<OptimizerData>,
    pub offset: AtomicUsize,
}

impl GlobalTableFunctionState for ParoOptimizersGlobalState {
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

fn paro_optimizers_bind(
    _input: &TableFunctionBindInput,
    return_types: &mut Vec<LogicalType>,
    names: &mut Vec<String>,
) -> Result<Option<Box<dyn TableFunctionBindData>>> {
    names.push("name".to_string());
    return_types.push(LogicalType::Varchar);

    names.push("enabled".to_string());
    return_types.push(LogicalType::Boolean);

    names.push("last_elapsed_us".to_string());
    return_types.push(LogicalType::BigInt);

    names.push("invocation_count".to_string());
    return_types.push(LogicalType::BigInt);

    Ok(Some(Box::new(ParoOptimizersBindData)))
}

fn paro_optimizers_init_global(
    _input: &TableFunctionInitInput,
) -> Result<Option<Box<dyn GlobalTableFunctionState>>> {
    Ok(Some(Box::new(ParoOptimizersGlobalState {
        entries: Vec::new(),
        offset: AtomicUsize::new(0),
    })))
}

fn paro_optimizers_function(
    input: &mut TableFunctionInput,
    output: &mut Chunk,
) -> Result<TableFunctionResult> {
    let output_allocator = output.allocator().clone();
    let gstate = input
        .global_state
        .and_then(|gs| gs.as_any().downcast_ref::<ParoOptimizersGlobalState>());
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
    let mut names = Vec::with_capacity(batch_size);
    let mut enabled = Vec::with_capacity(batch_size);
    let mut last_elapsed = Vec::with_capacity(batch_size);
    let mut invocations = Vec::with_capacity(batch_size);

    for entry in gstate.entries.iter().skip(offset).take(batch_size) {
        names.push(entry.name.clone());
        enabled.push(entry.enabled);
        last_elapsed.push(entry.last_elapsed_us);
        invocations.push(entry.invocation_count);
    }

    gstate.offset.fetch_add(batch_size, Ordering::Relaxed);

    let name_refs: Vec<&str> = names.iter().map(|value| value.as_str()).collect();
    if let Some(col) = output.column_mut(0) {
        *col = Vector::try_from_strings(&name_refs, output_allocator.clone())?;
    }
    if let Some(col) = output.column_mut(1) {
        *col = Vector::try_from_bool(&enabled, output_allocator.clone())?;
    }
    if let Some(col) = output.column_mut(2) {
        *col = Vector::try_from_i64(&last_elapsed, output_allocator.clone())?;
    }
    if let Some(col) = output.column_mut(3) {
        *col = Vector::try_from_i64(&invocations, output_allocator.clone())?;
    }
    output.set_cardinality(batch_size);

    if gstate.offset.load(Ordering::Relaxed) >= gstate.entries.len() {
        Ok(TableFunctionResult::Finished)
    } else {
        Ok(TableFunctionResult::HaveMoreOutput)
    }
}

fn paro_optimizers_progress(
    _bind_data: Option<&dyn TableFunctionBindData>,
    global_state: Option<&dyn GlobalTableFunctionState>,
) -> f64 {
    global_state
        .map(|state| state.get_progress())
        .unwrap_or(-1.0)
}

pub fn create_paro_optimizers_function_set() -> TableFunctionSet {
    let mut func = TableFunction::new("paro_optimizers", vec![]);
    func.bind = Some(paro_optimizers_bind);
    func.init_global = Some(paro_optimizers_init_global);
    func.function = Some(paro_optimizers_function);
    func.table_scan_progress = Some(paro_optimizers_progress);

    let mut set = TableFunctionSet::new("paro_optimizers");
    set.add_function(func);
    set
}

pub fn populate_optimizer_data(state: &mut ParoOptimizersGlobalState, entries: Vec<OptimizerData>) {
    state.entries = entries;
    state.offset.store(0, Ordering::Relaxed);
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::HashMap;

    use paro_common::runtime_value::Value;

    #[test]
    fn test_paro_optimizers_bind() {
        let mut return_types = Vec::new();
        let mut names = Vec::new();

        let bind = paro_optimizers_bind(
            &TableFunctionBindInput::new(&[], &HashMap::new()),
            &mut return_types,
            &mut names,
        )
        .unwrap();

        assert!(bind.is_some());
        assert_eq!(
            names,
            vec!["name", "enabled", "last_elapsed_us", "invocation_count"]
        );
        assert_eq!(
            return_types,
            vec![
                LogicalType::Varchar,
                LogicalType::Boolean,
                LogicalType::BigInt,
                LogicalType::BigInt,
            ]
        );
    }

    #[test]
    fn test_paro_optimizers_function_with_data() {
        let input = TableFunctionInitInput::new_for_test(None, &[]);
        let mut state_box = paro_optimizers_init_global(&input).unwrap().unwrap();
        let state = state_box
            .as_any_mut()
            .downcast_mut::<ParoOptimizersGlobalState>()
            .unwrap();

        populate_optimizer_data(
            state,
            vec![
                OptimizerData {
                    name: "filter_pushdown".to_string(),
                    enabled: true,
                    last_elapsed_us: 42,
                    invocation_count: 7,
                },
                OptimizerData {
                    name: "join_order".to_string(),
                    enabled: false,
                    last_elapsed_us: 0,
                    invocation_count: 0,
                },
            ],
        );

        let state_ref = state_box
            .as_any()
            .downcast_ref::<ParoOptimizersGlobalState>()
            .unwrap();
        let mut input = TableFunctionInput {
            bind_data: None,
            local_state: None,
            global_state: Some(state_ref),
        };
        let mut chunk = paro_common::test_utils::test_chunk_with_capacity(
            &[
                LogicalType::Varchar,
                LogicalType::Boolean,
                LogicalType::BigInt,
                LogicalType::BigInt,
            ],
            2048,
        );

        let result = paro_optimizers_function(&mut input, &mut chunk).unwrap();
        assert_eq!(result, TableFunctionResult::Finished);
        assert_eq!(chunk.size(), 2);
        assert_eq!(
            chunk.column(0).unwrap().get_value(0),
            Value::Varchar("filter_pushdown".to_string())
        );
        assert_eq!(chunk.column(1).unwrap().get_value(0), Value::Boolean(true));
        assert_eq!(chunk.column(2).unwrap().get_value(0), Value::BigInt(42));
        assert_eq!(chunk.column(3).unwrap().get_value(0), Value::BigInt(7));
        assert_eq!(
            chunk.column(0).unwrap().get_value(1),
            Value::Varchar("join_order".to_string())
        );
    }

    #[test]
    fn test_paro_optimizers_progress() {
        let input = TableFunctionInitInput::new_for_test(None, &[]);
        let mut state_box = paro_optimizers_init_global(&input).unwrap().unwrap();
        let state = state_box
            .as_any_mut()
            .downcast_mut::<ParoOptimizersGlobalState>()
            .unwrap();

        populate_optimizer_data(
            state,
            vec![OptimizerData {
                name: "filter_pushdown".to_string(),
                enabled: true,
                last_elapsed_us: 1,
                invocation_count: 1,
            }],
        );

        let state_ref = state_box
            .as_any()
            .downcast_ref::<ParoOptimizersGlobalState>()
            .unwrap();
        assert!((paro_optimizers_progress(None, Some(state_ref)) - 0.0).abs() < 0.001);
        state_ref.offset.store(1, Ordering::Relaxed);
        assert!((paro_optimizers_progress(None, Some(state_ref)) - 100.0).abs() < 0.001);
    }
}
