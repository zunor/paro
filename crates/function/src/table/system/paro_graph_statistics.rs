// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! paro_graph_statistics() Table Function
//!
//! ## Overview
//! Returns statistics for a specific property graph.
//!
//! ## Return Columns
//!
//! ## Example
//! ```sql
//! SELECT * FROM paro_graph_statistics('social_network');
//! ```

use std::any::Any;
use std::sync::atomic::{AtomicUsize, Ordering};

use paro_common::chunk::Chunk;
use paro_common::error::Result;
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use paro_common::vector::Vector;

use crate::table::{
    GlobalTableFunctionState, TableFunction, TableFunctionBindData, TableFunctionBindInput,
    TableFunctionInitInput, TableFunctionInput, TableFunctionResult, TableFunctionSet,
};

/// Bind data for paro_graph_statistics().
#[derive(Clone)]
pub struct ParoGraphStatisticsBindData {
    pub graph_name: String,
}

impl TableFunctionBindData for ParoGraphStatisticsBindData {
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

/// Data for a single statistics row.
#[derive(Debug, Clone)]
pub struct GraphStatisticsData {
    pub label: String,
    pub label_type: String,
    pub count: i64,
    pub avg_degree: Option<f64>,
    pub index_size_bytes: i64,
}

/// Global state for paro_graph_statistics().
pub struct ParoGraphStatisticsGlobalState {
    pub graph_name: String,
    pub entries: Vec<GraphStatisticsData>,
    pub offset: AtomicUsize,
}

impl GlobalTableFunctionState for ParoGraphStatisticsGlobalState {
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

fn paro_graph_statistics_bind(
    input: &TableFunctionBindInput,
    return_types: &mut Vec<LogicalType>,
    names: &mut Vec<String>,
) -> Result<Option<Box<dyn TableFunctionBindData>>> {
    let graph_name = if let Some(Value::Varchar(s)) = input.inputs.first() {
        s.clone()
    } else {
        return Err(paro_common::error::syntax(
            "paro_graph_statistics requires a VARCHAR argument (graph name)".to_string(),
        ));
    };

    names.push("label".to_string());
    return_types.push(LogicalType::Varchar);

    names.push("type".to_string());
    return_types.push(LogicalType::Varchar);

    names.push("count".to_string());
    return_types.push(LogicalType::BigInt);

    names.push("avg_degree".to_string());
    return_types.push(LogicalType::Double);

    names.push("index_size_bytes".to_string());
    return_types.push(LogicalType::BigInt);

    Ok(Some(Box::new(ParoGraphStatisticsBindData { graph_name })))
}

fn paro_graph_statistics_init_global(
    input: &TableFunctionInitInput,
) -> Result<Option<Box<dyn GlobalTableFunctionState>>> {
    let graph_name = input
        .bind_data
        .and_then(|bd| bd.as_any().downcast_ref::<ParoGraphStatisticsBindData>())
        .map(|bd| bd.graph_name.clone())
        .unwrap_or_default();

    Ok(Some(Box::new(ParoGraphStatisticsGlobalState {
        graph_name,
        entries: Vec::new(),
        offset: AtomicUsize::new(0),
    })))
}

fn paro_graph_statistics_function(
    input: &mut TableFunctionInput,
    output: &mut Chunk,
) -> Result<TableFunctionResult> {
    let output_allocator = output.allocator().clone();
    let gstate = input
        .global_state
        .and_then(|gs| gs.as_any().downcast_ref::<ParoGraphStatisticsGlobalState>());

    let gstate = match gstate {
        Some(gs) => gs,
        None => {
            output.set_cardinality(0);
            return Ok(TableFunctionResult::Finished);
        }
    };

    let offset = gstate.offset.load(Ordering::Relaxed);
    if offset >= gstate.entries.len() {
        output.set_cardinality(0);
        return Ok(TableFunctionResult::Finished);
    }

    let batch_size = 2048.min(gstate.entries.len() - offset);
    let mut count = 0;

    let mut labels = Vec::with_capacity(batch_size);
    let mut types = Vec::with_capacity(batch_size);
    let mut counts = Vec::with_capacity(batch_size);
    let mut avg_degrees = Vec::with_capacity(batch_size);
    let mut avg_degree_nulls = Vec::with_capacity(batch_size);
    let mut index_sizes = Vec::with_capacity(batch_size);

    for entry in gstate.entries.iter().skip(offset).take(batch_size) {
        labels.push(entry.label.clone());
        types.push(entry.label_type.clone());
        counts.push(entry.count);
        avg_degrees.push(entry.avg_degree.unwrap_or(0.0));
        avg_degree_nulls.push(entry.avg_degree.is_none());
        index_sizes.push(entry.index_size_bytes);
        count += 1;
    }

    gstate.offset.fetch_add(count, Ordering::Relaxed);

    if count > 0 {
        let refs: Vec<&str> = labels.iter().map(|s| s.as_str()).collect();
        if let Some(col) = output.column_mut(0) {
            *col = Vector::try_from_strings(&refs, output_allocator.clone())?;
        }
        let refs: Vec<&str> = types.iter().map(|s| s.as_str()).collect();
        if let Some(col) = output.column_mut(1) {
            *col = Vector::try_from_strings(&refs, output_allocator.clone())?;
        }
        if let Some(col) = output.column_mut(2) {
            *col = Vector::try_from_i64(&counts, output_allocator.clone())?;
        }
        if let Some(col) = output.column_mut(3) {
            let mut vec = Vector::try_from_f64(&avg_degrees, output_allocator.clone())?;
            for (i, is_null) in avg_degree_nulls.iter().enumerate() {
                if *is_null {
                    vec.set_null(i, true);
                }
            }
            *col = vec;
        }
        if let Some(col) = output.column_mut(4) {
            *col = Vector::try_from_i64(&index_sizes, output_allocator.clone())?;
        }
        output.set_cardinality(count);
    }

    let new_offset = gstate.offset.load(Ordering::Relaxed);
    if new_offset >= gstate.entries.len() {
        Ok(TableFunctionResult::Finished)
    } else {
        Ok(TableFunctionResult::HaveMoreOutput)
    }
}

/// Create the paro_graph_statistics() table function set.
pub fn create_paro_graph_statistics_function_set() -> TableFunctionSet {
    let mut func = TableFunction::new("paro_graph_statistics", vec![LogicalType::Varchar]);
    func.bind = Some(paro_graph_statistics_bind);
    func.init_global = Some(paro_graph_statistics_init_global);
    func.function = Some(paro_graph_statistics_function);

    let mut set = TableFunctionSet::new("paro_graph_statistics");
    set.add_function(func);
    set
}

/// Populate graph statistics data into the global state.
pub fn populate_graph_statistics_data(
    state: &mut ParoGraphStatisticsGlobalState,
    data: Vec<GraphStatisticsData>,
) {
    state.entries = data;
    state.offset.store(0, Ordering::Relaxed);
}
