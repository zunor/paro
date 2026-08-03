// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! paro_indexes() Table Function
//!
//!
//!
//! ## Overview
//! Returns information about all indexes in the database.
//!
//! ## Return Columns
//!
//! ## Example
//! ```sql
//! SELECT * FROM paro_indexes();
//! SELECT index_name FROM paro_indexes() WHERE table_name = 'users';
//! SELECT * FROM paro_indexes() WHERE is_unique;
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

/// Bind data for paro_indexes().
///
/// This is empty since paro_indexes() takes no arguments.
/// The actual index data is collected at init time.
#[derive(Clone)]
pub struct ParoIndexesBindData;

impl TableFunctionBindData for ParoIndexesBindData {
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

/// Index entry data collected from the catalog.
#[derive(Debug, Clone)]
pub struct IndexData {
    /// Database name
    pub database_name: String,
    /// Database OID
    pub database_oid: u64,
    /// Schema name
    pub schema_name: String,
    /// Schema OID
    pub schema_oid: u64,
    /// Index name
    pub index_name: String,
    /// Index OID
    pub index_oid: u64,
    /// Table name the index belongs to
    pub table_name: String,
    /// Table OID
    pub table_oid: u64,
    /// Whether this is a unique index
    pub is_unique: bool,
    /// Whether this is a primary key index
    pub is_primary: bool,
    /// Index type (ART, BPlusTree, etc.)
    pub index_type: String,
    /// Build state
    pub build_state: String,
    /// Number of indexed entries
    pub entry_count: i64,
    /// Extra type-specific information in JSON
    pub extra_info: String,
    /// SQL statement to recreate the index
    pub sql: Option<String>,
}

/// Global state for paro_indexes().
///
/// Contains the collected index data and current offset.
pub struct ParoIndexesGlobalState {
    /// Collected index entries
    pub entries: Vec<IndexData>,
    /// Current offset into entries
    pub offset: AtomicUsize,
}

impl GlobalTableFunctionState for ParoIndexesGlobalState {
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

/// Bind function for paro_indexes().
fn paro_indexes_bind(
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

    names.push("index_name".to_string());
    return_types.push(LogicalType::Varchar);

    names.push("index_oid".to_string());
    return_types.push(LogicalType::BigInt);

    names.push("table_name".to_string());
    return_types.push(LogicalType::Varchar);

    names.push("table_oid".to_string());
    return_types.push(LogicalType::BigInt);

    names.push("is_unique".to_string());
    return_types.push(LogicalType::Boolean);

    names.push("is_primary".to_string());
    return_types.push(LogicalType::Boolean);

    names.push("index_type".to_string());
    return_types.push(LogicalType::Varchar);

    names.push("build_state".to_string());
    return_types.push(LogicalType::Varchar);

    names.push("entry_count".to_string());
    return_types.push(LogicalType::BigInt);

    names.push("extra_info".to_string());
    return_types.push(LogicalType::Varchar);

    names.push("sql".to_string());
    return_types.push(LogicalType::Varchar);

    Ok(Some(Box::new(ParoIndexesBindData)))
}

/// Init global function for paro_indexes().
///
/// Note: This function cannot access the catalog directly because
/// table functions don't have access to the execution context at init time.
/// The index data must be injected via a different mechanism.
///
/// For now, we return an empty state. The actual index data will be
/// populated by the executor when it has access to the catalog.
fn paro_indexes_init_global(
    _input: &TableFunctionInitInput,
) -> Result<Option<Box<dyn GlobalTableFunctionState>>> {
    // Create empty state - will be populated by executor
    Ok(Some(Box::new(ParoIndexesGlobalState {
        entries: Vec::new(),
        offset: AtomicUsize::new(0),
    })))
}

/// Main function for paro_indexes().
fn paro_indexes_function(
    input: &mut TableFunctionInput,
    output: &mut Chunk,
) -> Result<TableFunctionResult> {
    let output_allocator = output.allocator().clone();
    let gstate = input
        .global_state
        .and_then(|gs| gs.as_any().downcast_ref::<ParoIndexesGlobalState>());

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
    let mut index_names = Vec::with_capacity(batch_size);
    let mut index_oids = Vec::with_capacity(batch_size);
    let mut table_names = Vec::with_capacity(batch_size);
    let mut table_oids = Vec::with_capacity(batch_size);
    let mut is_uniques = Vec::with_capacity(batch_size);
    let mut is_primaries = Vec::with_capacity(batch_size);
    let mut index_types = Vec::with_capacity(batch_size);
    let mut build_states = Vec::with_capacity(batch_size);
    let mut entry_counts = Vec::with_capacity(batch_size);
    let mut extra_infos = Vec::with_capacity(batch_size);
    let mut sqls = Vec::with_capacity(batch_size);

    for entry in gstate.entries.iter().skip(offset).take(batch_size) {
        db_names.push(entry.database_name.clone());
        db_oids.push(entry.database_oid as i64);
        schema_names.push(entry.schema_name.clone());
        schema_oids.push(entry.schema_oid as i64);
        index_names.push(entry.index_name.clone());
        index_oids.push(entry.index_oid as i64);
        table_names.push(entry.table_name.clone());
        table_oids.push(entry.table_oid as i64);
        is_uniques.push(entry.is_unique);
        is_primaries.push(entry.is_primary);
        index_types.push(entry.index_type.clone());
        build_states.push(entry.build_state.clone());
        entry_counts.push(entry.entry_count);
        extra_infos.push(entry.extra_info.clone());
        sqls.push(entry.sql.clone().unwrap_or_default());
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

        // Column 4: index_name (VARCHAR)
        let index_name_refs: Vec<&str> = index_names.iter().map(|s| s.as_str()).collect();
        let index_name_vec = Vector::try_from_strings(&index_name_refs, output_allocator.clone())?;
        if let Some(col) = output.column_mut(4) {
            *col = index_name_vec;
        }

        // Column 5: index_oid (BIGINT)
        let index_oid_vec = Vector::try_from_i64(&index_oids, output_allocator.clone())?;
        if let Some(col) = output.column_mut(5) {
            *col = index_oid_vec;
        }

        // Column 6: table_name (VARCHAR)
        let table_name_refs: Vec<&str> = table_names.iter().map(|s| s.as_str()).collect();
        let table_name_vec = Vector::try_from_strings(&table_name_refs, output_allocator.clone())?;
        if let Some(col) = output.column_mut(6) {
            *col = table_name_vec;
        }

        // Column 7: table_oid (BIGINT)
        let table_oid_vec = Vector::try_from_i64(&table_oids, output_allocator.clone())?;
        if let Some(col) = output.column_mut(7) {
            *col = table_oid_vec;
        }

        // Column 8: is_unique (BOOLEAN)
        let is_unique_vec = Vector::try_from_bool(&is_uniques, output_allocator.clone())?;
        if let Some(col) = output.column_mut(8) {
            *col = is_unique_vec;
        }

        // Column 9: is_primary (BOOLEAN)
        let is_primary_vec = Vector::try_from_bool(&is_primaries, output_allocator.clone())?;
        if let Some(col) = output.column_mut(9) {
            *col = is_primary_vec;
        }

        // Column 10: index_type (VARCHAR)
        let index_type_refs: Vec<&str> = index_types.iter().map(|s| s.as_str()).collect();
        let index_type_vec = Vector::try_from_strings(&index_type_refs, output_allocator.clone())?;
        if let Some(col) = output.column_mut(10) {
            *col = index_type_vec;
        }

        // Column 11: build_state (VARCHAR)
        let build_state_refs: Vec<&str> = build_states.iter().map(|s| s.as_str()).collect();
        let build_state_vec =
            Vector::try_from_strings(&build_state_refs, output_allocator.clone())?;
        if let Some(col) = output.column_mut(11) {
            *col = build_state_vec;
        }

        // Column 12: entry_count (BIGINT)
        let entry_count_vec = Vector::try_from_i64(&entry_counts, output_allocator.clone())?;
        if let Some(col) = output.column_mut(12) {
            *col = entry_count_vec;
        }

        // Column 13: extra_info (VARCHAR)
        let extra_info_refs: Vec<&str> = extra_infos.iter().map(|s| s.as_str()).collect();
        let extra_info_vec = Vector::try_from_strings(&extra_info_refs, output_allocator.clone())?;
        if let Some(col) = output.column_mut(13) {
            *col = extra_info_vec;
        }

        // Column 14: sql (VARCHAR)
        let sql_refs: Vec<&str> = sqls.iter().map(|s| s.as_str()).collect();
        let sql_vec = Vector::try_from_strings(&sql_refs, output_allocator.clone())?;
        if let Some(col) = output.column_mut(14) {
            *col = sql_vec;
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

/// Progress function for paro_indexes().
fn paro_indexes_progress(
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

/// Create the paro_indexes() table function set.
pub fn create_paro_indexes_function_set() -> TableFunctionSet {
    let mut func = TableFunction::new("paro_indexes", vec![]);

    func.bind = Some(paro_indexes_bind);
    func.init_global = Some(paro_indexes_init_global);
    func.function = Some(paro_indexes_function);
    func.table_scan_progress = Some(paro_indexes_progress);

    let mut set = TableFunctionSet::new("paro_indexes");
    set.add_function(func);
    set
}

/// Populate index data into the global state.
///
/// This is called by the executor when it has access to the catalog.
/// The executor should call this after creating the global state.
pub fn populate_index_data(state: &mut ParoIndexesGlobalState, indexes: Vec<IndexData>) {
    state.entries = indexes;
    state.offset.store(0, Ordering::Relaxed);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::table::TableFunctionInitInput;

    #[test]
    fn test_paro_indexes_bind() {
        let empty_map = std::collections::HashMap::new();
        let input = TableFunctionBindInput::new(&[], &empty_map);
        let mut return_types = Vec::new();
        let mut names = Vec::new();

        let result = paro_indexes_bind(&input, &mut return_types, &mut names);
        assert!(result.is_ok());

        // Verify columns
        assert_eq!(names.len(), 15);
        assert_eq!(names[0], "database_name");
        assert_eq!(names[1], "database_oid");
        assert_eq!(names[2], "schema_name");
        assert_eq!(names[3], "schema_oid");
        assert_eq!(names[4], "index_name");
        assert_eq!(names[5], "index_oid");
        assert_eq!(names[6], "table_name");
        assert_eq!(names[7], "table_oid");
        assert_eq!(names[8], "is_unique");
        assert_eq!(names[9], "is_primary");
        assert_eq!(names[10], "index_type");
        assert_eq!(names[11], "build_state");
        assert_eq!(names[12], "entry_count");
        assert_eq!(names[13], "extra_info");
        assert_eq!(names[14], "sql");

        assert_eq!(return_types.len(), 15);
        assert_eq!(return_types[0], LogicalType::Varchar);
        assert_eq!(return_types[1], LogicalType::BigInt);
        assert_eq!(return_types[2], LogicalType::Varchar);
        assert_eq!(return_types[3], LogicalType::BigInt);
        assert_eq!(return_types[4], LogicalType::Varchar);
        assert_eq!(return_types[5], LogicalType::BigInt);
        assert_eq!(return_types[6], LogicalType::Varchar);
        assert_eq!(return_types[7], LogicalType::BigInt);
        assert_eq!(return_types[8], LogicalType::Boolean);
        assert_eq!(return_types[9], LogicalType::Boolean);
        assert_eq!(return_types[10], LogicalType::Varchar);
        assert_eq!(return_types[11], LogicalType::Varchar);
        assert_eq!(return_types[12], LogicalType::BigInt);
        assert_eq!(return_types[13], LogicalType::Varchar);
        assert_eq!(return_types[14], LogicalType::Varchar);
    }

    #[test]
    fn test_paro_indexes_init_global() {
        let input = TableFunctionInitInput::new_for_test(None, &[]);
        let result = paro_indexes_init_global(&input);
        assert!(result.is_ok());

        let state = result.unwrap();
        assert!(state.is_some());
    }

    #[test]
    fn test_paro_indexes_function_empty() {
        let input = TableFunctionInitInput::new_for_test(None, &[]);
        let state_box = paro_indexes_init_global(&input).unwrap().unwrap();
        let state = state_box
            .as_any()
            .downcast_ref::<ParoIndexesGlobalState>()
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
                LogicalType::Varchar, // index_name
                LogicalType::BigInt,  // index_oid
                LogicalType::Varchar, // table_name
                LogicalType::BigInt,  // table_oid
                LogicalType::Boolean, // is_unique
                LogicalType::Boolean, // is_primary
                LogicalType::Varchar, // index_type
                LogicalType::Varchar, // sql
            ],
            2048,
        );

        let result = paro_indexes_function(&mut func_input, &mut chunk);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), TableFunctionResult::Finished);
        assert_eq!(chunk.size(), 0);
    }

    #[test]
    fn test_paro_indexes_function_with_data() {
        use paro_common::runtime_value::Value;

        let input = TableFunctionInitInput::new_for_test(None, &[]);
        let mut state_box = paro_indexes_init_global(&input).unwrap().unwrap();

        // Populate with test data
        let state = state_box
            .as_any_mut()
            .downcast_mut::<ParoIndexesGlobalState>()
            .unwrap();

        populate_index_data(
            state,
            vec![
                IndexData {
                    database_name: "test_db".to_string(),
                    database_oid: 1,
                    schema_name: "public".to_string(),
                    schema_oid: 10,
                    index_name: "users_pkey".to_string(),
                    index_oid: 200,
                    table_name: "users".to_string(),
                    table_oid: 100,
                    is_unique: true,
                    is_primary: true,
                    index_type: "ART".to_string(),
                    build_state: "BUILT".to_string(),
                    entry_count: 100,
                    extra_info: "{\"columns\":[\"id\"]}".to_string(),
                    sql: Some("CREATE UNIQUE INDEX users_pkey ON users(id)".to_string()),
                },
                IndexData {
                    database_name: "test_db".to_string(),
                    database_oid: 1,
                    schema_name: "public".to_string(),
                    schema_oid: 10,
                    index_name: "users_email_idx".to_string(),
                    index_oid: 201,
                    table_name: "users".to_string(),
                    table_oid: 100,
                    is_unique: true,
                    is_primary: false,
                    index_type: "ART".to_string(),
                    build_state: "BUILDING".to_string(),
                    entry_count: 90,
                    extra_info: "{\"columns\":[\"email\"]}".to_string(),
                    sql: Some("CREATE UNIQUE INDEX users_email_idx ON users(email)".to_string()),
                },
                IndexData {
                    database_name: "test_db".to_string(),
                    database_oid: 1,
                    schema_name: "public".to_string(),
                    schema_oid: 10,
                    index_name: "orders_user_idx".to_string(),
                    index_oid: 202,
                    table_name: "orders".to_string(),
                    table_oid: 101,
                    is_unique: false,
                    is_primary: false,
                    index_type: "ART".to_string(),
                    build_state: "FAILED".to_string(),
                    entry_count: 0,
                    extra_info: "{\"failure_reason\":\"test\"}".to_string(),
                    sql: None,
                },
            ],
        );

        let state_ref = state_box
            .as_any()
            .downcast_ref::<ParoIndexesGlobalState>()
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
                LogicalType::Varchar, // index_name
                LogicalType::BigInt,  // index_oid
                LogicalType::Varchar, // table_name
                LogicalType::BigInt,  // table_oid
                LogicalType::Boolean, // is_unique
                LogicalType::Boolean, // is_primary
                LogicalType::Varchar, // index_type
                LogicalType::Varchar, // build_state
                LogicalType::BigInt,  // entry_count
                LogicalType::Varchar, // extra_info
                LogicalType::Varchar, // sql
            ],
            2048,
        );

        let result = paro_indexes_function(&mut func_input, &mut chunk);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), TableFunctionResult::Finished);
        assert_eq!(chunk.size(), 3);

        // Verify data
        let index_name_col = chunk.column(4).unwrap();
        assert_eq!(
            index_name_col.get_value(0),
            Value::Varchar("users_pkey".to_string())
        );
        assert_eq!(
            index_name_col.get_value(1),
            Value::Varchar("users_email_idx".to_string())
        );
        assert_eq!(
            index_name_col.get_value(2),
            Value::Varchar("orders_user_idx".to_string())
        );

        let is_unique_col = chunk.column(8).unwrap();
        assert_eq!(is_unique_col.get_value(0), Value::Boolean(true));
        assert_eq!(is_unique_col.get_value(1), Value::Boolean(true));
        assert_eq!(is_unique_col.get_value(2), Value::Boolean(false));

        let is_primary_col = chunk.column(9).unwrap();
        assert_eq!(is_primary_col.get_value(0), Value::Boolean(true));
        assert_eq!(is_primary_col.get_value(1), Value::Boolean(false));
        assert_eq!(is_primary_col.get_value(2), Value::Boolean(false));

        let index_type_col = chunk.column(10).unwrap();
        assert_eq!(
            index_type_col.get_value(0),
            Value::Varchar("ART".to_string())
        );

        let build_state_col = chunk.column(11).unwrap();
        assert_eq!(
            build_state_col.get_value(0),
            Value::Varchar("BUILT".to_string())
        );

        let entry_count_col = chunk.column(12).unwrap();
        assert_eq!(entry_count_col.get_value(0), Value::BigInt(100));
    }

    #[test]
    fn test_paro_indexes_progress() {
        let input = TableFunctionInitInput::new_for_test(None, &[]);
        let mut state_box = paro_indexes_init_global(&input).unwrap().unwrap();

        // Empty state should show 100% progress
        let state = state_box
            .as_any()
            .downcast_ref::<ParoIndexesGlobalState>()
            .unwrap();
        assert!((paro_indexes_progress(None, Some(state)) - 100.0).abs() < 0.001);

        // Add data and check progress
        let state_mut = state_box
            .as_any_mut()
            .downcast_mut::<ParoIndexesGlobalState>()
            .unwrap();
        populate_index_data(
            state_mut,
            vec![
                IndexData {
                    database_name: "test".to_string(),
                    database_oid: 1,
                    schema_name: "public".to_string(),
                    schema_oid: 10,
                    index_name: "idx1".to_string(),
                    index_oid: 200,
                    table_name: "t1".to_string(),
                    table_oid: 100,
                    is_unique: false,
                    is_primary: false,
                    index_type: "ART".to_string(),
                    build_state: "BUILT".to_string(),
                    entry_count: 1,
                    extra_info: "{}".to_string(),
                    sql: None,
                },
                IndexData {
                    database_name: "test".to_string(),
                    database_oid: 1,
                    schema_name: "public".to_string(),
                    schema_oid: 10,
                    index_name: "idx2".to_string(),
                    index_oid: 201,
                    table_name: "t2".to_string(),
                    table_oid: 101,
                    is_unique: true,
                    is_primary: false,
                    index_type: "ART".to_string(),
                    build_state: "BUILT".to_string(),
                    entry_count: 2,
                    extra_info: "{}".to_string(),
                    sql: None,
                },
            ],
        );

        let state = state_box
            .as_any()
            .downcast_ref::<ParoIndexesGlobalState>()
            .unwrap();

        // 0% progress at start
        assert!((paro_indexes_progress(None, Some(state)) - 0.0).abs() < 0.001);

        // Advance offset
        state.offset.store(1, Ordering::Relaxed);
        assert!((paro_indexes_progress(None, Some(state)) - 50.0).abs() < 0.001);

        // Complete
        state.offset.store(2, Ordering::Relaxed);
        assert!((paro_indexes_progress(None, Some(state)) - 100.0).abs() < 0.001);
    }

    #[test]
    fn test_create_paro_indexes_function_set() {
        let set = create_paro_indexes_function_set();
        assert_eq!(set.name, "paro_indexes");
        assert_eq!(set.functions.len(), 1);

        let func = &set.functions[0];
        assert_eq!(func.name, "paro_indexes");
        assert!(func.arguments.is_empty());
        assert!(func.bind.is_some());
        assert!(func.init_global.is_some());
        assert!(func.function.is_some());
        assert!(func.table_scan_progress.is_some());
    }
}
