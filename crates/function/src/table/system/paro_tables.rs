// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! paro_tables() Table Function
//!
//!
//!
//! ## Overview
//! Returns information about all tables in the database.
//!
//! ## Return Columns
//!
//! ## Example
//! ```sql
//! SELECT * FROM paro_tables();
//! SELECT table_name FROM paro_tables() WHERE NOT internal;
//! SELECT * FROM paro_tables() WHERE schema_name = 'public';
//! ```
//!
//! ## Dependencies Check
//! - Catalog: ✅ `paro_catalog`
//! - TableFunction: ✅ `crate::table`

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

// ============================================================================
// Bind Data
// ============================================================================

/// Bind data for paro_tables().
///
/// This is empty since paro_tables() takes no arguments.
/// The actual table data is collected at init time.
#[derive(Clone)]
pub struct ParoTablesBindData;

impl TableFunctionBindData for ParoTablesBindData {
    fn clone_box(&self) -> Box<dyn TableFunctionBindData> {
        Box::new(self.clone())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn cardinality(&self) -> Option<usize> {
        // Unknown at bind time, will be determined at init time
        None
    }
}

// ============================================================================
// Global State
// ============================================================================

/// Table entry data collected from the catalog.
#[derive(Debug, Clone)]
pub struct TableData {
    /// Database name
    pub database_name: String,
    /// Database OID
    pub database_oid: u64,
    /// Schema name
    pub schema_name: String,
    /// Schema OID
    pub schema_oid: u64,
    /// Table name
    pub table_name: String,
    /// Table OID
    pub table_oid: u64,
    /// Whether this is an internal table
    pub internal: bool,
    /// Whether this is a temporary table
    pub temporary: bool,
    /// Number of columns
    pub column_count: i64,
    /// Number of indexes
    pub index_count: i64,
    /// Estimated visible rows
    pub estimated_rows: i64,
    /// Estimated visible storage size in bytes
    pub estimated_size_bytes: i64,
}

/// Global state for paro_tables().
///
/// Contains the collected table data and current offset.
pub struct ParoTablesGlobalState {
    /// Collected table entries
    pub entries: Vec<TableData>,
    /// Current offset into entries
    pub offset: AtomicUsize,
}

impl GlobalTableFunctionState for ParoTablesGlobalState {
    fn max_threads(&self) -> usize {
        1 // Single-threaded scan
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

// ============================================================================
// Function Implementation
// ============================================================================

/// Bind function for paro_tables().
fn paro_tables_bind(
    _input: &TableFunctionBindInput,
    return_types: &mut Vec<LogicalType>,
    names: &mut Vec<String>,
) -> Result<Option<Box<dyn TableFunctionBindData>>> {
    names.push("database_name".to_string());
    return_types.push(LogicalType::Varchar);

    names.push("database_oid".to_string());
    return_types.push(LogicalType::BigInt);

    names.push("schema_name".to_string());
    return_types.push(LogicalType::Varchar);

    names.push("schema_oid".to_string());
    return_types.push(LogicalType::BigInt);

    names.push("table_name".to_string());
    return_types.push(LogicalType::Varchar);

    names.push("table_oid".to_string());
    return_types.push(LogicalType::BigInt);

    names.push("internal".to_string());
    return_types.push(LogicalType::Boolean);

    names.push("temporary".to_string());
    return_types.push(LogicalType::Boolean);

    names.push("column_count".to_string());
    return_types.push(LogicalType::BigInt);

    names.push("index_count".to_string());
    return_types.push(LogicalType::BigInt);

    names.push("estimated_rows".to_string());
    return_types.push(LogicalType::BigInt);

    names.push("estimated_size_bytes".to_string());
    return_types.push(LogicalType::BigInt);

    Ok(Some(Box::new(ParoTablesBindData)))
}

/// Init global function for paro_tables().
///
/// Note: This function cannot access the catalog directly because
/// table functions don't have access to the execution context at init time.
/// The table data must be injected via a different mechanism.
///
/// For now, we return an empty state. The actual table data will be
/// populated by the executor when it has access to the catalog.
fn paro_tables_init_global(
    _input: &TableFunctionInitInput,
) -> Result<Option<Box<dyn GlobalTableFunctionState>>> {
    // Create empty state - will be populated by executor
    Ok(Some(Box::new(ParoTablesGlobalState {
        entries: Vec::new(),
        offset: AtomicUsize::new(0),
    })))
}

/// Main function for paro_tables().
fn paro_tables_function(
    input: &mut TableFunctionInput,
    output: &mut Chunk,
) -> Result<TableFunctionResult> {
    let output_allocator = output.allocator().clone();
    let gstate = input
        .global_state
        .and_then(|gs| gs.as_any().downcast_ref::<ParoTablesGlobalState>());

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

    // Fill output chunk
    let batch_size = 2048.min(gstate.entries.len() - offset);
    let mut count = 0;

    // Collect values for each column
    let mut db_names = Vec::with_capacity(batch_size);
    let mut db_oids = Vec::with_capacity(batch_size);
    let mut schema_names = Vec::with_capacity(batch_size);
    let mut schema_oids = Vec::with_capacity(batch_size);
    let mut table_names = Vec::with_capacity(batch_size);
    let mut table_oids = Vec::with_capacity(batch_size);
    let mut internals = Vec::with_capacity(batch_size);
    let mut temporaries = Vec::with_capacity(batch_size);
    let mut column_counts = Vec::with_capacity(batch_size);
    let mut index_counts = Vec::with_capacity(batch_size);
    let mut estimated_rows = Vec::with_capacity(batch_size);
    let mut estimated_sizes = Vec::with_capacity(batch_size);

    for entry in gstate.entries.iter().skip(offset).take(batch_size) {
        db_names.push(entry.database_name.clone());
        db_oids.push(entry.database_oid as i64);
        schema_names.push(entry.schema_name.clone());
        schema_oids.push(entry.schema_oid as i64);
        table_names.push(entry.table_name.clone());
        table_oids.push(entry.table_oid as i64);
        internals.push(entry.internal);
        temporaries.push(entry.temporary);
        column_counts.push(entry.column_count);
        index_counts.push(entry.index_count);
        estimated_rows.push(entry.estimated_rows);
        estimated_sizes.push(entry.estimated_size_bytes);
        count += 1;
    }

    // Update offset
    gstate.offset.fetch_add(count, Ordering::Relaxed);

    // Set column values
    if count > 0 {
        // Column 0: database_name (VARCHAR)
        let db_name_refs: Vec<&str> = db_names.iter().map(|s| s.as_str()).collect();
        let db_name_vec = Vector::try_from_strings(&db_name_refs, output_allocator.clone())?;
        if let Some(col) = output.column_mut(0) {
            *col = db_name_vec;
        }

        // Column 1: database_oid (BIGINT)
        let db_oid_vec = Vector::try_from_i64(&db_oids, output_allocator.clone())?;
        if let Some(col) = output.column_mut(1) {
            *col = db_oid_vec;
        }

        // Column 2: schema_name (VARCHAR)
        let schema_name_refs: Vec<&str> = schema_names.iter().map(|s| s.as_str()).collect();
        let schema_name_vec =
            Vector::try_from_strings(&schema_name_refs, output_allocator.clone())?;
        if let Some(col) = output.column_mut(2) {
            *col = schema_name_vec;
        }

        // Column 3: schema_oid (BIGINT)
        let schema_oid_vec = Vector::try_from_i64(&schema_oids, output_allocator.clone())?;
        if let Some(col) = output.column_mut(3) {
            *col = schema_oid_vec;
        }

        // Column 4: table_name (VARCHAR)
        let table_name_refs: Vec<&str> = table_names.iter().map(|s| s.as_str()).collect();
        let table_name_vec = Vector::try_from_strings(&table_name_refs, output_allocator.clone())?;
        if let Some(col) = output.column_mut(4) {
            *col = table_name_vec;
        }

        // Column 5: table_oid (BIGINT)
        let table_oid_vec = Vector::try_from_i64(&table_oids, output_allocator.clone())?;
        if let Some(col) = output.column_mut(5) {
            *col = table_oid_vec;
        }

        // Column 6: internal (BOOLEAN)
        let internal_vec = Vector::try_from_bool(&internals, output_allocator.clone())?;
        if let Some(col) = output.column_mut(6) {
            *col = internal_vec;
        }

        // Column 7: temporary (BOOLEAN)
        let temporary_vec = Vector::try_from_bool(&temporaries, output_allocator.clone())?;
        if let Some(col) = output.column_mut(7) {
            *col = temporary_vec;
        }

        // Column 8: column_count (BIGINT)
        let column_count_vec = Vector::try_from_i64(&column_counts, output_allocator.clone())?;
        if let Some(col) = output.column_mut(8) {
            *col = column_count_vec;
        }

        // Column 9: index_count (BIGINT)
        let index_count_vec = Vector::try_from_i64(&index_counts, output_allocator.clone())?;
        if let Some(col) = output.column_mut(9) {
            *col = index_count_vec;
        }

        // Column 10: estimated_rows (BIGINT)
        let estimated_rows_vec = Vector::try_from_i64(&estimated_rows, output_allocator.clone())?;
        if let Some(col) = output.column_mut(10) {
            *col = estimated_rows_vec;
        }

        // Column 11: estimated_size_bytes (BIGINT)
        let estimated_sizes_vec = Vector::try_from_i64(&estimated_sizes, output_allocator.clone())?;
        if let Some(col) = output.column_mut(11) {
            *col = estimated_sizes_vec;
        }

        output.set_cardinality(count);
    }

    // Check if we have more data
    let new_offset = gstate.offset.load(Ordering::Relaxed);
    if new_offset >= gstate.entries.len() {
        Ok(TableFunctionResult::Finished)
    } else {
        Ok(TableFunctionResult::HaveMoreOutput)
    }
}

/// Progress function for paro_tables().
fn paro_tables_progress(
    _bind_data: Option<&dyn TableFunctionBindData>,
    global_state: Option<&dyn GlobalTableFunctionState>,
) -> f64 {
    match global_state {
        Some(gs) => gs.get_progress(),
        None => -1.0,
    }
}

// ============================================================================
// Public API
// ============================================================================

/// Create the paro_tables() table function set.
pub fn create_paro_tables_function_set() -> TableFunctionSet {
    let mut func = TableFunction::new("paro_tables", vec![]);

    func.bind = Some(paro_tables_bind);
    func.init_global = Some(paro_tables_init_global);
    func.function = Some(paro_tables_function);
    func.table_scan_progress = Some(paro_tables_progress);

    let mut set = TableFunctionSet::new("paro_tables");
    set.add_function(func);
    set
}

/// Populate table data into the global state.
///
/// This is called by the executor when it has access to the catalog.
/// The executor should call this after creating the global state.
pub fn populate_table_data(state: &mut ParoTablesGlobalState, tables: Vec<TableData>) {
    state.entries = tables;
    state.offset.store(0, Ordering::Relaxed);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::table::TableFunctionInitInput;

    #[test]
    fn test_paro_tables_bind() {
        let empty_map = std::collections::HashMap::new();
        let input = TableFunctionBindInput::new(&[], &empty_map);
        let mut return_types = Vec::new();
        let mut names = Vec::new();

        let result = paro_tables_bind(&input, &mut return_types, &mut names);
        assert!(result.is_ok());

        // Verify columns
        assert_eq!(names.len(), 12);
        assert_eq!(names[0], "database_name");
        assert_eq!(names[1], "database_oid");
        assert_eq!(names[2], "schema_name");
        assert_eq!(names[3], "schema_oid");
        assert_eq!(names[4], "table_name");
        assert_eq!(names[5], "table_oid");
        assert_eq!(names[6], "internal");
        assert_eq!(names[7], "temporary");
        assert_eq!(names[8], "column_count");
        assert_eq!(names[9], "index_count");
        assert_eq!(names[10], "estimated_rows");
        assert_eq!(names[11], "estimated_size_bytes");

        assert_eq!(return_types.len(), 12);
        assert_eq!(return_types[0], LogicalType::Varchar);
        assert_eq!(return_types[1], LogicalType::BigInt);
        assert_eq!(return_types[2], LogicalType::Varchar);
        assert_eq!(return_types[3], LogicalType::BigInt);
        assert_eq!(return_types[4], LogicalType::Varchar);
        assert_eq!(return_types[5], LogicalType::BigInt);
        assert_eq!(return_types[6], LogicalType::Boolean);
        assert_eq!(return_types[7], LogicalType::Boolean);
        assert_eq!(return_types[8], LogicalType::BigInt);
        assert_eq!(return_types[9], LogicalType::BigInt);
        assert_eq!(return_types[10], LogicalType::BigInt);
        assert_eq!(return_types[11], LogicalType::BigInt);
    }

    #[test]
    fn test_paro_tables_init_global() {
        let input = TableFunctionInitInput::new_for_test(None, &[]);
        let result = paro_tables_init_global(&input);
        assert!(result.is_ok());

        let state = result.unwrap();
        assert!(state.is_some());
    }

    #[test]
    fn test_paro_tables_function_empty() {
        let input = TableFunctionInitInput::new_for_test(None, &[]);
        let state_box = paro_tables_init_global(&input).unwrap().unwrap();
        let state = state_box
            .as_any()
            .downcast_ref::<ParoTablesGlobalState>()
            .unwrap();

        // Empty state should return finished immediately
        let mut func_input = TableFunctionInput {
            bind_data: None,
            local_state: None,
            global_state: Some(state),
        };

        let mut chunk = paro_common::test_utils::test_chunk_with_capacity(
            &[
                LogicalType::Varchar, // database_name
                LogicalType::BigInt,  // database_oid
                LogicalType::Varchar, // schema_name
                LogicalType::BigInt,  // schema_oid
                LogicalType::Varchar, // table_name
                LogicalType::BigInt,  // table_oid
                LogicalType::Boolean, // internal
                LogicalType::Boolean, // temporary
                LogicalType::BigInt,  // column_count
                LogicalType::BigInt,  // index_count
            ],
            2048,
        );

        let result = paro_tables_function(&mut func_input, &mut chunk);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), TableFunctionResult::Finished);
        assert_eq!(chunk.size(), 0);
    }

    #[test]
    fn test_paro_tables_function_with_data() {
        use paro_common::runtime_value::Value;

        let input = TableFunctionInitInput::new_for_test(None, &[]);
        let mut state_box = paro_tables_init_global(&input).unwrap().unwrap();

        // Populate with test data
        let state = state_box
            .as_any_mut()
            .downcast_mut::<ParoTablesGlobalState>()
            .unwrap();

        populate_table_data(
            state,
            vec![
                TableData {
                    database_name: "test_db".to_string(),
                    database_oid: 1,
                    schema_name: "public".to_string(),
                    schema_oid: 10,
                    table_name: "users".to_string(),
                    table_oid: 100,
                    internal: false,
                    temporary: false,
                    column_count: 5,
                    index_count: 2,
                    estimated_rows: 128,
                    estimated_size_bytes: 4096,
                },
                TableData {
                    database_name: "test_db".to_string(),
                    database_oid: 1,
                    schema_name: "public".to_string(),
                    schema_oid: 10,
                    table_name: "orders".to_string(),
                    table_oid: 101,
                    internal: false,
                    temporary: true,
                    column_count: 8,
                    index_count: 1,
                    estimated_rows: 64,
                    estimated_size_bytes: 2048,
                },
                TableData {
                    database_name: "test_db".to_string(),
                    database_oid: 1,
                    schema_name: "pg_catalog".to_string(),
                    schema_oid: 11,
                    table_name: "pg_type".to_string(),
                    table_oid: 102,
                    internal: true,
                    temporary: false,
                    column_count: 10,
                    index_count: 0,
                    estimated_rows: 32,
                    estimated_size_bytes: 1024,
                },
            ],
        );

        let state_ref = state_box
            .as_any()
            .downcast_ref::<ParoTablesGlobalState>()
            .unwrap();

        let mut func_input = TableFunctionInput {
            bind_data: None,
            local_state: None,
            global_state: Some(state_ref),
        };

        let mut chunk = paro_common::test_utils::test_chunk_with_capacity(
            &[
                LogicalType::Varchar, // database_name
                LogicalType::BigInt,  // database_oid
                LogicalType::Varchar, // schema_name
                LogicalType::BigInt,  // schema_oid
                LogicalType::Varchar, // table_name
                LogicalType::BigInt,  // table_oid
                LogicalType::Boolean, // internal
                LogicalType::Boolean, // temporary
                LogicalType::BigInt,  // column_count
                LogicalType::BigInt,  // index_count
                LogicalType::BigInt,  // estimated_rows
                LogicalType::BigInt,  // estimated_size_bytes
            ],
            2048,
        );

        let result = paro_tables_function(&mut func_input, &mut chunk);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), TableFunctionResult::Finished);
        assert_eq!(chunk.size(), 3);

        // Verify data
        let table_name_col = chunk.column(4).unwrap();
        assert_eq!(
            table_name_col.get_value(0),
            Value::Varchar("users".to_string())
        );
        assert_eq!(
            table_name_col.get_value(1),
            Value::Varchar("orders".to_string())
        );
        assert_eq!(
            table_name_col.get_value(2),
            Value::Varchar("pg_type".to_string())
        );

        let internal_col = chunk.column(6).unwrap();
        assert_eq!(internal_col.get_value(0), Value::Boolean(false));
        assert_eq!(internal_col.get_value(1), Value::Boolean(false));
        assert_eq!(internal_col.get_value(2), Value::Boolean(true));

        let temporary_col = chunk.column(7).unwrap();
        assert_eq!(temporary_col.get_value(0), Value::Boolean(false));
        assert_eq!(temporary_col.get_value(1), Value::Boolean(true));
        assert_eq!(temporary_col.get_value(2), Value::Boolean(false));

        let column_count_col = chunk.column(8).unwrap();
        assert_eq!(column_count_col.get_value(0), Value::BigInt(5));
        assert_eq!(column_count_col.get_value(1), Value::BigInt(8));
        assert_eq!(column_count_col.get_value(2), Value::BigInt(10));

        let estimated_rows_col = chunk.column(10).unwrap();
        assert_eq!(estimated_rows_col.get_value(0), Value::BigInt(128));
        assert_eq!(estimated_rows_col.get_value(1), Value::BigInt(64));
        assert_eq!(estimated_rows_col.get_value(2), Value::BigInt(32));
    }

    #[test]
    fn test_paro_tables_progress() {
        let input = TableFunctionInitInput::new_for_test(None, &[]);
        let mut state_box = paro_tables_init_global(&input).unwrap().unwrap();

        // Empty state should show 100% progress
        let state = state_box
            .as_any()
            .downcast_ref::<ParoTablesGlobalState>()
            .unwrap();
        assert!((paro_tables_progress(None, Some(state)) - 100.0).abs() < 0.001);

        // Add data and check progress
        let state_mut = state_box
            .as_any_mut()
            .downcast_mut::<ParoTablesGlobalState>()
            .unwrap();
        populate_table_data(
            state_mut,
            vec![
                TableData {
                    database_name: "test".to_string(),
                    database_oid: 1,
                    schema_name: "public".to_string(),
                    schema_oid: 10,
                    table_name: "t1".to_string(),
                    table_oid: 100,
                    internal: false,
                    temporary: false,
                    column_count: 3,
                    index_count: 0,
                    estimated_rows: 10,
                    estimated_size_bytes: 100,
                },
                TableData {
                    database_name: "test".to_string(),
                    database_oid: 1,
                    schema_name: "public".to_string(),
                    schema_oid: 10,
                    table_name: "t2".to_string(),
                    table_oid: 101,
                    internal: false,
                    temporary: false,
                    column_count: 5,
                    index_count: 1,
                    estimated_rows: 20,
                    estimated_size_bytes: 200,
                },
            ],
        );

        let state = state_box
            .as_any()
            .downcast_ref::<ParoTablesGlobalState>()
            .unwrap();

        // 0% progress at start
        assert!((paro_tables_progress(None, Some(state)) - 0.0).abs() < 0.001);

        // Advance offset
        state.offset.store(1, Ordering::Relaxed);
        assert!((paro_tables_progress(None, Some(state)) - 50.0).abs() < 0.001);

        // Complete
        state.offset.store(2, Ordering::Relaxed);
        assert!((paro_tables_progress(None, Some(state)) - 100.0).abs() < 0.001);
    }

    #[test]
    fn test_create_paro_tables_function_set() {
        let set = create_paro_tables_function_set();
        assert_eq!(set.name, "paro_tables");
        assert_eq!(set.functions.len(), 1);

        let func = &set.functions[0];
        assert_eq!(func.name, "paro_tables");
        assert!(func.arguments.is_empty());
        assert!(func.bind.is_some());
        assert!(func.init_global.is_some());
        assert!(func.function.is_some());
        assert!(func.table_scan_progress.is_some());
    }

    #[test]
    fn test_paro_tables_large_batch() {
        let input = TableFunctionInitInput::new_for_test(None, &[]);
        let mut state_box = paro_tables_init_global(&input).unwrap().unwrap();

        // Create many tables to test batching
        let state = state_box
            .as_any_mut()
            .downcast_mut::<ParoTablesGlobalState>()
            .unwrap();

        let mut tables = Vec::new();
        for i in 0..3000 {
            tables.push(TableData {
                database_name: "test_db".to_string(),
                database_oid: 1,
                schema_name: "public".to_string(),
                schema_oid: 10,
                table_name: format!("table_{}", i),
                table_oid: 100 + i as u64,
                internal: false,
                temporary: false,
                column_count: 5,
                index_count: 0,
                estimated_rows: i as i64,
                estimated_size_bytes: (i as i64) * 10,
            });
        }
        populate_table_data(state, tables);

        let state_ref = state_box
            .as_any()
            .downcast_ref::<ParoTablesGlobalState>()
            .unwrap();

        let mut func_input = TableFunctionInput {
            bind_data: None,
            local_state: None,
            global_state: Some(state_ref),
        };

        let mut chunk = paro_common::test_utils::test_chunk_with_capacity(
            &[
                LogicalType::Varchar,
                LogicalType::BigInt,
                LogicalType::Varchar,
                LogicalType::BigInt,
                LogicalType::Varchar,
                LogicalType::BigInt,
                LogicalType::Boolean,
                LogicalType::Boolean,
                LogicalType::BigInt,
                LogicalType::BigInt,
                LogicalType::BigInt,
                LogicalType::BigInt,
            ],
            2048,
        );

        // First batch should return 2048 rows
        let result = paro_tables_function(&mut func_input, &mut chunk);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), TableFunctionResult::HaveMoreOutput);
        assert_eq!(chunk.size(), 2048);

        // Second batch should return remaining 952 rows
        let result = paro_tables_function(&mut func_input, &mut chunk);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), TableFunctionResult::Finished);
        assert_eq!(chunk.size(), 952);
    }
}
