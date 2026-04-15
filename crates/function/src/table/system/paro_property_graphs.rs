//! paro_property_graphs() Table Function
//!
//! ## Overview
//! Returns information about all property graphs in the database.
//!
//! ## Return Columns
//!
//! ## Example
//! ```sql
//! SELECT * FROM paro_property_graphs();
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

/// Bind data for paro_property_graphs().
#[derive(Clone)]
pub struct ParoPropertyGraphsBindData;

impl TableFunctionBindData for ParoPropertyGraphsBindData {
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

/// Data for a single property graph entry.
#[derive(Debug, Clone)]
pub struct PropertyGraphData {
    pub graph_name: String,
    pub vertex_tables: String,
    pub edge_tables: String,
    pub state: String,
    pub vertex_count: i64,
    pub edge_count: i64,
    pub delta_size: i64,
    pub last_rebuild_micros: Option<i64>,
    pub fingerprint: String,
    pub index_size_bytes: i64,
}

/// Global state for paro_property_graphs().
pub struct ParoPropertyGraphsGlobalState {
    pub entries: Vec<PropertyGraphData>,
    pub offset: AtomicUsize,
}

impl GlobalTableFunctionState for ParoPropertyGraphsGlobalState {
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

fn paro_property_graphs_bind(
    _input: &TableFunctionBindInput,
    return_types: &mut Vec<LogicalType>,
    names: &mut Vec<String>,
) -> Result<Option<Box<dyn TableFunctionBindData>>> {
    names.push("graph_name".to_string());
    return_types.push(LogicalType::Varchar);

    names.push("vertex_tables".to_string());
    return_types.push(LogicalType::Varchar);

    names.push("edge_tables".to_string());
    return_types.push(LogicalType::Varchar);

    names.push("vertex_count".to_string());
    return_types.push(LogicalType::BigInt);

    names.push("edge_count".to_string());
    return_types.push(LogicalType::BigInt);

    names.push("state".to_string());
    return_types.push(LogicalType::Varchar);

    names.push("delta_size".to_string());
    return_types.push(LogicalType::BigInt);

    names.push("last_rebuild".to_string());
    return_types.push(LogicalType::Timestamp);

    names.push("fingerprint".to_string());
    return_types.push(LogicalType::Varchar);

    names.push("index_size_bytes".to_string());
    return_types.push(LogicalType::BigInt);

    Ok(Some(Box::new(ParoPropertyGraphsBindData)))
}

fn paro_property_graphs_init_global(
    _input: &TableFunctionInitInput,
) -> Result<Option<Box<dyn GlobalTableFunctionState>>> {
    Ok(Some(Box::new(ParoPropertyGraphsGlobalState {
        entries: Vec::new(),
        offset: AtomicUsize::new(0),
    })))
}

fn paro_property_graphs_function(
    input: &mut TableFunctionInput,
    output: &mut Chunk,
) -> Result<TableFunctionResult> {
    let gstate = input
        .global_state
        .and_then(|gs| gs.as_any().downcast_ref::<ParoPropertyGraphsGlobalState>());

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

    let mut graph_names = Vec::with_capacity(batch_size);
    let mut vertex_tables = Vec::with_capacity(batch_size);
    let mut edge_tables = Vec::with_capacity(batch_size);
    let mut vertex_counts = Vec::with_capacity(batch_size);
    let mut edge_counts = Vec::with_capacity(batch_size);
    let mut states = Vec::with_capacity(batch_size);
    let mut delta_sizes = Vec::with_capacity(batch_size);
    let mut last_rebuilds = Vec::with_capacity(batch_size);
    let mut fingerprints = Vec::with_capacity(batch_size);
    let mut index_sizes = Vec::with_capacity(batch_size);

    for entry in gstate.entries.iter().skip(offset).take(batch_size) {
        graph_names.push(entry.graph_name.clone());
        vertex_tables.push(entry.vertex_tables.clone());
        edge_tables.push(entry.edge_tables.clone());
        vertex_counts.push(entry.vertex_count);
        edge_counts.push(entry.edge_count);
        states.push(entry.state.clone());
        delta_sizes.push(entry.delta_size);
        last_rebuilds.push(entry.last_rebuild_micros);
        fingerprints.push(entry.fingerprint.clone());
        index_sizes.push(entry.index_size_bytes);
        count += 1;
    }

    gstate.offset.fetch_add(count, Ordering::Relaxed);

    if count > 0 {
        let refs: Vec<&str> = graph_names.iter().map(|s| s.as_str()).collect();
        if let Some(col) = output.column_mut(0) {
            *col = Vector::from_strings(&refs);
        }
        let refs: Vec<&str> = vertex_tables.iter().map(|s| s.as_str()).collect();
        if let Some(col) = output.column_mut(1) {
            *col = Vector::from_strings(&refs);
        }
        let refs: Vec<&str> = edge_tables.iter().map(|s| s.as_str()).collect();
        if let Some(col) = output.column_mut(2) {
            *col = Vector::from_strings(&refs);
        }
        if let Some(col) = output.column_mut(3) {
            *col = Vector::from_i64(&vertex_counts);
        }
        if let Some(col) = output.column_mut(4) {
            *col = Vector::from_i64(&edge_counts);
        }
        if let Some(col) = output.column_mut(5) {
            let refs: Vec<&str> = states.iter().map(|s| s.as_str()).collect();
            *col = Vector::from_strings(&refs);
        }
        if let Some(col) = output.column_mut(6) {
            *col = Vector::from_i64(&delta_sizes);
        }
        if let Some(col) = output.column_mut(7) {
            let mut ts = Vector::with_capacity(LogicalType::Timestamp, count);
            ts.set_len(count);
            for (idx, value) in last_rebuilds.iter().enumerate() {
                match value {
                    Some(micros) => ts.set_value(idx, &Value::Timestamp(*micros)),
                    None => ts.set_null(idx, true),
                }
            }
            *col = ts;
        }
        if let Some(col) = output.column_mut(8) {
            let refs: Vec<&str> = fingerprints.iter().map(|s| s.as_str()).collect();
            *col = Vector::from_strings(&refs);
        }
        if let Some(col) = output.column_mut(9) {
            *col = Vector::from_i64(&index_sizes);
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

/// Create the paro_property_graphs() table function set.
pub fn create_paro_property_graphs_function_set() -> TableFunctionSet {
    let mut func = TableFunction::new("paro_property_graphs", vec![]);
    func.bind = Some(paro_property_graphs_bind);
    func.init_global = Some(paro_property_graphs_init_global);
    func.function = Some(paro_property_graphs_function);

    let mut set = TableFunctionSet::new("paro_property_graphs");
    set.add_function(func);
    set
}

/// Populate property graph data into the global state.
pub fn populate_property_graph_data(
    state: &mut ParoPropertyGraphsGlobalState,
    data: Vec<PropertyGraphData>,
) {
    state.entries = data;
    state.offset.store(0, Ordering::Relaxed);
}
