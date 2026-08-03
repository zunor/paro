// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! paro_schemas() Table Function
//!
//!
//!
//! ## Overview
//! Returns information about all schemas in the database.
//!
//! ## Return Columns
//!
//! ## Example
//! ```sql
//! SELECT * FROM paro_schemas();
//! SELECT schema_name FROM paro_schemas() WHERE NOT internal;
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

/// Bind data for paro_schemas().
///
/// This is empty since paro_schemas() takes no arguments.
/// The actual schema data is collected at init time.
#[derive(Clone)]
pub struct ParoSchemasBindData;

impl TableFunctionBindData for ParoSchemasBindData {
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

/// Schema entry data collected from the catalog.
#[derive(Debug, Clone)]
pub struct SchemaData {
    /// Schema OID
    pub oid: u64,
    /// Database name
    pub database_name: String,
    /// Database OID (same as schema OID for now)
    pub database_oid: u64,
    /// Schema name
    pub schema_name: String,
    /// Whether this is an internal schema
    pub internal: bool,
}

/// Global state for paro_schemas().
///
/// Contains the collected schema data and current offset.
pub struct ParoSchemasGlobalState {
    /// Collected schema entries
    pub entries: Vec<SchemaData>,
    /// Current offset into entries
    pub offset: AtomicUsize,
}

impl GlobalTableFunctionState for ParoSchemasGlobalState {
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

/// Bind function for paro_schemas().
fn paro_schemas_bind(
    _input: &TableFunctionBindInput,
    return_types: &mut Vec<LogicalType>,
    names: &mut Vec<String>,
) -> Result<Option<Box<dyn TableFunctionBindData>>> {
    // Define return columns
    names.push("oid".to_string());
    return_types.push(LogicalType::BigInt);

    names.push("database_name".to_string());
    return_types.push(LogicalType::Varchar);

    names.push("database_oid".to_string());
    return_types.push(LogicalType::BigInt);

    names.push("schema_name".to_string());
    return_types.push(LogicalType::Varchar);

    names.push("internal".to_string());
    return_types.push(LogicalType::Boolean);

    Ok(Some(Box::new(ParoSchemasBindData)))
}

/// Init global function for paro_schemas().
///
/// Note: This function cannot access the catalog directly because
/// table functions don't have access to the execution context at init time.
/// The schema data must be injected via a different mechanism.
///
/// For now, we return an empty state. The actual schema data will be
/// populated by the executor when it has access to the catalog.
fn paro_schemas_init_global(
    _input: &TableFunctionInitInput,
) -> Result<Option<Box<dyn GlobalTableFunctionState>>> {
    // Create empty state - will be populated by executor
    Ok(Some(Box::new(ParoSchemasGlobalState {
        entries: Vec::new(),
        offset: AtomicUsize::new(0),
    })))
}

/// Main function for paro_schemas().
fn paro_schemas_function(
    input: &mut TableFunctionInput,
    output: &mut Chunk,
) -> Result<TableFunctionResult> {
    let output_allocator = output.allocator().clone();
    let gstate = input
        .global_state
        .and_then(|gs| gs.as_any().downcast_ref::<ParoSchemasGlobalState>());

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
    let mut oids = Vec::with_capacity(batch_size);
    let mut db_names = Vec::with_capacity(batch_size);
    let mut db_oids = Vec::with_capacity(batch_size);
    let mut schema_names = Vec::with_capacity(batch_size);
    let mut internals = Vec::with_capacity(batch_size);

    for entry in gstate.entries.iter().skip(offset).take(batch_size) {
        oids.push(entry.oid as i64);
        db_names.push(entry.database_name.clone());
        db_oids.push(entry.database_oid as i64);
        schema_names.push(entry.schema_name.clone());
        internals.push(entry.internal);
        count += 1;
    }

    // Update offset
    gstate.offset.fetch_add(count, Ordering::Relaxed);

    // Set column values
    if count > 0 {
        // Column 0: oid (BIGINT)
        let oid_vec = Vector::try_from_i64(&oids, output_allocator.clone())?;
        if let Some(col) = output.column_mut(0) {
            *col = oid_vec;
        }

        // Column 1: database_name (VARCHAR)
        let db_name_refs: Vec<&str> = db_names.iter().map(|s| s.as_str()).collect();
        let db_name_vec = Vector::try_from_strings(&db_name_refs, output_allocator.clone())?;
        if let Some(col) = output.column_mut(1) {
            *col = db_name_vec;
        }

        // Column 2: database_oid (BIGINT)
        let db_oid_vec = Vector::try_from_i64(&db_oids, output_allocator.clone())?;
        if let Some(col) = output.column_mut(2) {
            *col = db_oid_vec;
        }

        // Column 3: schema_name (VARCHAR)
        let schema_name_refs: Vec<&str> = schema_names.iter().map(|s| s.as_str()).collect();
        let schema_name_vec =
            Vector::try_from_strings(&schema_name_refs, output_allocator.clone())?;
        if let Some(col) = output.column_mut(3) {
            *col = schema_name_vec;
        }

        // Column 4: internal (BOOLEAN)
        let internal_vec = Vector::try_from_bool(&internals, output_allocator.clone())?;
        if let Some(col) = output.column_mut(4) {
            *col = internal_vec;
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

/// Progress function for paro_schemas().
fn paro_schemas_progress(
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

/// Create the paro_schemas() table function set.
pub fn create_paro_schemas_function_set() -> TableFunctionSet {
    let mut func = TableFunction::new("paro_schemas", vec![]);

    func.bind = Some(paro_schemas_bind);
    func.init_global = Some(paro_schemas_init_global);
    func.function = Some(paro_schemas_function);
    func.table_scan_progress = Some(paro_schemas_progress);

    let mut set = TableFunctionSet::new("paro_schemas");
    set.add_function(func);
    set
}

/// Populate schema data into the global state.
///
/// This is called by the executor when it has access to the catalog.
/// The executor should call this after creating the global state.
pub fn populate_schema_data(state: &mut ParoSchemasGlobalState, schemas: Vec<SchemaData>) {
    state.entries = schemas;
    state.offset.store(0, Ordering::Relaxed);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::table::TableFunctionInitInput;
    use paro_common::runtime_value::Value;

    #[test]
    fn test_paro_schemas_bind() {
        let empty_map = std::collections::HashMap::new();
        let input = TableFunctionBindInput::new(&[], &empty_map);
        let mut return_types = Vec::new();
        let mut names = Vec::new();

        let result = paro_schemas_bind(&input, &mut return_types, &mut names);
        assert!(result.is_ok());

        // Verify columns
        assert_eq!(names.len(), 5);
        assert_eq!(names[0], "oid");
        assert_eq!(names[1], "database_name");
        assert_eq!(names[2], "database_oid");
        assert_eq!(names[3], "schema_name");
        assert_eq!(names[4], "internal");

        assert_eq!(return_types.len(), 5);
        assert_eq!(return_types[0], LogicalType::BigInt);
        assert_eq!(return_types[1], LogicalType::Varchar);
        assert_eq!(return_types[2], LogicalType::BigInt);
        assert_eq!(return_types[3], LogicalType::Varchar);
        assert_eq!(return_types[4], LogicalType::Boolean);
    }

    #[test]
    fn test_paro_schemas_init_global() {
        let input = TableFunctionInitInput::new_for_test(None, &[]);
        let result = paro_schemas_init_global(&input);
        assert!(result.is_ok());

        let state = result.unwrap();
        assert!(state.is_some());
    }

    #[test]
    fn test_paro_schemas_function_empty() {
        let input = TableFunctionInitInput::new_for_test(None, &[]);
        let state_box = paro_schemas_init_global(&input).unwrap().unwrap();
        let state = state_box
            .as_any()
            .downcast_ref::<ParoSchemasGlobalState>()
            .unwrap();

        // Empty state should return finished immediately
        let mut func_input = TableFunctionInput {
            bind_data: None,
            local_state: None,
            global_state: Some(state),
        };

        let mut chunk = paro_common::test_utils::test_chunk_with_capacity(
            &[
                LogicalType::BigInt,
                LogicalType::Varchar,
                LogicalType::BigInt,
                LogicalType::Varchar,
                LogicalType::Boolean,
            ],
            2048,
        );

        let result = paro_schemas_function(&mut func_input, &mut chunk);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), TableFunctionResult::Finished);
        assert_eq!(chunk.size(), 0);
    }

    #[test]
    fn test_paro_schemas_function_with_data() {
        let input = TableFunctionInitInput::new_for_test(None, &[]);
        let mut state_box = paro_schemas_init_global(&input).unwrap().unwrap();

        // Populate with test data
        let state = state_box
            .as_any_mut()
            .downcast_mut::<ParoSchemasGlobalState>()
            .unwrap();

        populate_schema_data(
            state,
            vec![
                SchemaData {
                    oid: 1,
                    database_name: "test_db".to_string(),
                    database_oid: 0,
                    schema_name: "public".to_string(),
                    internal: false,
                },
                SchemaData {
                    oid: 2,
                    database_name: "test_db".to_string(),
                    database_oid: 0,
                    schema_name: "pg_catalog".to_string(),
                    internal: true,
                },
            ],
        );

        let state_ref = state_box
            .as_any()
            .downcast_ref::<ParoSchemasGlobalState>()
            .unwrap();

        let mut func_input = TableFunctionInput {
            bind_data: None,
            local_state: None,
            global_state: Some(state_ref),
        };

        let mut chunk = paro_common::test_utils::test_chunk_with_capacity(
            &[
                LogicalType::BigInt,
                LogicalType::Varchar,
                LogicalType::BigInt,
                LogicalType::Varchar,
                LogicalType::Boolean,
            ],
            2048,
        );

        let result = paro_schemas_function(&mut func_input, &mut chunk);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), TableFunctionResult::Finished);
        assert_eq!(chunk.size(), 2);

        // Verify data
        let oid_col = chunk.column(0).unwrap();
        assert_eq!(oid_col.get_value(0), Value::BigInt(1));
        assert_eq!(oid_col.get_value(1), Value::BigInt(2));

        let schema_name_col = chunk.column(3).unwrap();
        assert_eq!(
            schema_name_col.get_value(0),
            Value::Varchar("public".to_string())
        );
        assert_eq!(
            schema_name_col.get_value(1),
            Value::Varchar("pg_catalog".to_string())
        );

        let internal_col = chunk.column(4).unwrap();
        assert_eq!(internal_col.get_value(0), Value::Boolean(false));
        assert_eq!(internal_col.get_value(1), Value::Boolean(true));
    }

    #[test]
    fn test_paro_schemas_progress() {
        let input = TableFunctionInitInput::new_for_test(None, &[]);
        let mut state_box = paro_schemas_init_global(&input).unwrap().unwrap();

        // Empty state should show 100% progress
        let state = state_box
            .as_any()
            .downcast_ref::<ParoSchemasGlobalState>()
            .unwrap();
        assert!((paro_schemas_progress(None, Some(state)) - 100.0).abs() < 0.001);

        // Add data and check progress
        let state_mut = state_box
            .as_any_mut()
            .downcast_mut::<ParoSchemasGlobalState>()
            .unwrap();
        populate_schema_data(
            state_mut,
            vec![
                SchemaData {
                    oid: 1,
                    database_name: "test".to_string(),
                    database_oid: 0,
                    schema_name: "s1".to_string(),
                    internal: false,
                },
                SchemaData {
                    oid: 2,
                    database_name: "test".to_string(),
                    database_oid: 0,
                    schema_name: "s2".to_string(),
                    internal: false,
                },
            ],
        );

        let state = state_box
            .as_any()
            .downcast_ref::<ParoSchemasGlobalState>()
            .unwrap();

        // 0% progress at start
        assert!((paro_schemas_progress(None, Some(state)) - 0.0).abs() < 0.001);

        // Advance offset
        state.offset.store(1, Ordering::Relaxed);
        assert!((paro_schemas_progress(None, Some(state)) - 50.0).abs() < 0.001);

        // Complete
        state.offset.store(2, Ordering::Relaxed);
        assert!((paro_schemas_progress(None, Some(state)) - 100.0).abs() < 0.001);
    }

    #[test]
    fn test_create_paro_schemas_function_set() {
        let set = create_paro_schemas_function_set();
        assert_eq!(set.name, "paro_schemas");
        assert_eq!(set.functions.len(), 1);

        let func = &set.functions[0];
        assert_eq!(func.name, "paro_schemas");
        assert!(func.arguments.is_empty());
        assert!(func.bind.is_some());
        assert!(func.init_global.is_some());
        assert!(func.function.is_some());
        assert!(func.table_scan_progress.is_some());
    }
}
