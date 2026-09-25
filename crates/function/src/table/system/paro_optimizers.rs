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
    pub kind: String,
    pub last_elapsed_us: i64,
    pub metric_value: i64,
    pub metric_unit: String,
    pub invocation_count: i64,
    /// Versioned machine record.  Human metric columns remain available for
    /// optimizer counters, but receipt consumers must use this typed payload
    /// instead of reconstructing enums from names and integers.
    pub record_type: String,
    pub record_id: u64,
    pub payload_json: Option<String>,
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

    names.push("kind".to_string());
    return_types.push(LogicalType::Varchar);

    names.push("last_elapsed_us".to_string());
    return_types.push(LogicalType::BigInt);

    names.push("metric_value".to_string());
    return_types.push(LogicalType::BigInt);

    names.push("metric_unit".to_string());
    return_types.push(LogicalType::Varchar);

    names.push("invocation_count".to_string());
    return_types.push(LogicalType::BigInt);

    names.push("record_type".to_string());
    return_types.push(LogicalType::Varchar);

    names.push("record_id".to_string());
    return_types.push(LogicalType::BigInt);

    names.push("payload_json".to_string());
    return_types.push(LogicalType::Varchar);

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
    let mut kinds = Vec::with_capacity(batch_size);
    let mut last_elapsed = Vec::with_capacity(batch_size);
    let mut metric_values = Vec::with_capacity(batch_size);
    let mut metric_units = Vec::with_capacity(batch_size);
    let mut invocations = Vec::with_capacity(batch_size);
    let mut record_types = Vec::with_capacity(batch_size);
    let mut record_ids = Vec::with_capacity(batch_size);
    let mut payloads = Vec::with_capacity(batch_size);

    for entry in gstate.entries.iter().skip(offset).take(batch_size) {
        names.push(entry.name.clone());
        kinds.push(entry.kind.clone());
        last_elapsed.push(entry.last_elapsed_us);
        metric_values.push(entry.metric_value);
        metric_units.push(entry.metric_unit.clone());
        invocations.push(entry.invocation_count);
        record_types.push(entry.record_type.clone());
        record_ids.push(entry.record_id);
        payloads.push(entry.payload_json.clone().unwrap_or_default());
    }

    gstate.offset.fetch_add(batch_size, Ordering::Relaxed);

    let name_refs: Vec<&str> = names.iter().map(|value| value.as_str()).collect();
    if let Some(col) = output.column_mut(0) {
        *col = Vector::try_from_strings(&name_refs, output_allocator.clone())?;
    }
    if let Some(col) = output.column_mut(1) {
        let kind_refs: Vec<&str> = kinds.iter().map(|value| value.as_str()).collect();
        *col = Vector::try_from_strings(&kind_refs, output_allocator.clone())?;
    }
    if let Some(col) = output.column_mut(2) {
        *col = Vector::try_from_i64(&last_elapsed, output_allocator.clone())?;
    }
    if let Some(col) = output.column_mut(3) {
        *col = Vector::try_from_i64(&metric_values, output_allocator.clone())?;
    }
    if let Some(col) = output.column_mut(4) {
        let unit_refs: Vec<&str> = metric_units.iter().map(|value| value.as_str()).collect();
        *col = Vector::try_from_strings(&unit_refs, output_allocator.clone())?;
    }
    if let Some(col) = output.column_mut(5) {
        *col = Vector::try_from_i64(&invocations, output_allocator.clone())?;
    }
    if let Some(col) = output.column_mut(6) {
        let refs: Vec<&str> = record_types.iter().map(String::as_str).collect();
        *col = Vector::try_from_strings(&refs, output_allocator.clone())?;
    }
    if let Some(col) = output.column_mut(7) {
        let ids: Vec<i64> = record_ids
            .iter()
            .map(|id| (*id).min(i64::MAX as u64) as i64)
            .collect();
        *col = Vector::try_from_i64(&ids, output_allocator.clone())?;
    }
    if let Some(col) = output.column_mut(8) {
        let refs: Vec<&str> = payloads.iter().map(String::as_str).collect();
        *col = Vector::try_from_strings(&refs, output_allocator)?;
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
            vec![
                "name",
                "kind",
                "last_elapsed_us",
                "metric_value",
                "metric_unit",
                "invocation_count",
                "record_type",
                "record_id",
                "payload_json"
            ]
        );
        assert_eq!(
            return_types,
            vec![
                LogicalType::Varchar,
                LogicalType::Varchar,
                LogicalType::BigInt,
                LogicalType::BigInt,
                LogicalType::Varchar,
                LogicalType::BigInt,
                LogicalType::Varchar,
                LogicalType::BigInt,
                LogicalType::Varchar,
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
                    name: "semantic_normalization".to_string(),
                    kind: "frontend".to_string(),
                    last_elapsed_us: 42,
                    metric_value: 7,
                    metric_unit: "invocations".to_string(),
                    invocation_count: 7,
                    record_type: "metric".to_string(),
                    record_id: 0,
                    payload_json: None,
                },
                OptimizerData {
                    name: "memo_exploration".to_string(),
                    kind: "search".to_string(),
                    last_elapsed_us: 0,
                    metric_value: 0,
                    metric_unit: "invocations".to_string(),
                    invocation_count: 0,
                    record_type: "metric".to_string(),
                    record_id: 0,
                    payload_json: None,
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
                LogicalType::Varchar,
                LogicalType::BigInt,
                LogicalType::BigInt,
                LogicalType::Varchar,
                LogicalType::BigInt,
                LogicalType::Varchar,
                LogicalType::BigInt,
                LogicalType::Varchar,
            ],
            2048,
        );

        let result = paro_optimizers_function(&mut input, &mut chunk).unwrap();
        assert_eq!(result, TableFunctionResult::Finished);
        assert_eq!(chunk.size(), 2);
        assert_eq!(
            chunk.column(0).unwrap().get_value(0),
            Value::Varchar("semantic_normalization".to_string())
        );
        assert_eq!(
            chunk.column(1).unwrap().get_value(0),
            Value::Varchar("frontend".to_string())
        );
        assert_eq!(chunk.column(2).unwrap().get_value(0), Value::BigInt(42));
        assert_eq!(chunk.column(3).unwrap().get_value(0), Value::BigInt(7));
        assert_eq!(
            chunk.column(4).unwrap().get_value(0),
            Value::Varchar("invocations".to_string())
        );
        assert_eq!(chunk.column(5).unwrap().get_value(0), Value::BigInt(7));
        assert_eq!(
            chunk.column(6).unwrap().get_value(0),
            Value::Varchar("metric".to_string())
        );
        assert_eq!(chunk.column(7).unwrap().get_value(0), Value::BigInt(0));
        assert_eq!(
            chunk.column(8).unwrap().get_value(0),
            Value::Varchar("".to_string())
        );
        assert_eq!(
            chunk.column(0).unwrap().get_value(1),
            Value::Varchar("memo_exploration".to_string())
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
                name: "semantic_normalization".to_string(),
                kind: "frontend".to_string(),
                last_elapsed_us: 1,
                metric_value: 1,
                metric_unit: "invocations".to_string(),
                invocation_count: 1,
                record_type: "metric".to_string(),
                record_id: 0,
                payload_json: None,
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
