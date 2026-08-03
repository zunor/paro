// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! paro_databases() Table Function
//!
//!
//!
//! ## Overview
//! Returns information about all databases registered in the DatabaseRegistry.
//!
//! ## Return Columns
//!
//! ## Example
//! ```sql
//! SELECT * FROM paro_databases();
//! SELECT database_name FROM paro_databases() WHERE allow_conn;
//! ```
//!
//! ## Dependencies Check
//! - DatabaseRegistry: ✅ `paro_instance::DatabaseRegistry`
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

/// Bind data for paro_databases().
///
/// This is empty since paro_databases() takes no arguments.
/// The actual database data is collected at init time.
#[derive(Clone)]
pub struct ParoDatabasesBindData;

impl TableFunctionBindData for ParoDatabasesBindData {
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

/// Database entry data collected from the DatabaseRegistry.
#[derive(Debug, Clone)]
pub struct DatabaseData {
    /// Database OID
    pub database_oid: u64,
    /// Database name
    pub database_name: String,
    /// Owner OID (stub: 0)
    pub owner_oid: u64,
    /// Encoding ID (6 = UTF8)
    pub encoding: i32,
    /// Collation
    pub collate: String,
    /// Character type
    pub ctype: String,
    /// Whether this is a template database
    pub is_template: bool,
    /// Whether connections are allowed
    pub allow_conn: bool,
    /// Connection limit (-1 = unlimited)
    pub conn_limit: i32,
    /// Access control list (NULL represented as empty)
    pub acl: Option<String>,
}

impl Default for DatabaseData {
    fn default() -> Self {
        Self {
            database_oid: 0,
            database_name: String::new(),
            owner_oid: 0,
            encoding: 6, // UTF8
            collate: "C".to_string(),
            ctype: "C".to_string(),
            is_template: false,
            allow_conn: true,
            conn_limit: -1,
            acl: None,
        }
    }
}

/// Global state for paro_databases().
///
/// Contains the collected database data and current offset.
pub struct ParoDatabasesGlobalState {
    /// Collected database entries
    pub entries: Vec<DatabaseData>,
    /// Current offset into entries
    pub offset: AtomicUsize,
}

impl GlobalTableFunctionState for ParoDatabasesGlobalState {
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

/// Bind function for paro_databases().
fn paro_databases_bind(
    _input: &TableFunctionBindInput,
    return_types: &mut Vec<LogicalType>,
    names: &mut Vec<String>,
) -> Result<Option<Box<dyn TableFunctionBindData>>> {
    // Define return columns matching PostgreSQL's pg_database structure
    names.push("database_oid".to_string());
    return_types.push(LogicalType::BigInt);

    names.push("database_name".to_string());
    return_types.push(LogicalType::Varchar);

    names.push("owner_oid".to_string());
    return_types.push(LogicalType::BigInt);

    names.push("encoding".to_string());
    return_types.push(LogicalType::Integer);

    names.push("collate".to_string());
    return_types.push(LogicalType::Varchar);

    names.push("ctype".to_string());
    return_types.push(LogicalType::Varchar);

    names.push("is_template".to_string());
    return_types.push(LogicalType::Boolean);

    names.push("allow_conn".to_string());
    return_types.push(LogicalType::Boolean);

    names.push("conn_limit".to_string());
    return_types.push(LogicalType::Integer);

    names.push("acl".to_string());
    return_types.push(LogicalType::Varchar);

    Ok(Some(Box::new(ParoDatabasesBindData)))
}

/// Init global function for paro_databases().
///
/// Note: This function cannot access the DatabaseRegistry directly because
/// table functions don't have access to the execution context at init time.
/// The database data must be injected via `populate_database_data`.
///
/// For now, we return an empty state. The actual database data will be
/// populated by the executor when it has access to the DatabaseRegistry.
fn paro_databases_init_global(
    _input: &TableFunctionInitInput,
) -> Result<Option<Box<dyn GlobalTableFunctionState>>> {
    // Create empty state - will be populated by executor
    Ok(Some(Box::new(ParoDatabasesGlobalState {
        entries: Vec::new(),
        offset: AtomicUsize::new(0),
    })))
}

/// Main function for paro_databases().
fn paro_databases_function(
    input: &mut TableFunctionInput,
    output: &mut Chunk,
) -> Result<TableFunctionResult> {
    let output_allocator = output.allocator().clone();
    let gstate = input
        .global_state
        .and_then(|gs| gs.as_any().downcast_ref::<ParoDatabasesGlobalState>());

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
    let mut db_oids = Vec::with_capacity(batch_size);
    let mut db_names = Vec::with_capacity(batch_size);
    let mut owner_oids = Vec::with_capacity(batch_size);
    let mut encodings = Vec::with_capacity(batch_size);
    let mut collates = Vec::with_capacity(batch_size);
    let mut ctypes = Vec::with_capacity(batch_size);
    let mut is_templates = Vec::with_capacity(batch_size);
    let mut allow_conns = Vec::with_capacity(batch_size);
    let mut conn_limits = Vec::with_capacity(batch_size);
    let mut acls: Vec<Option<&str>> = Vec::with_capacity(batch_size);

    for entry in gstate.entries.iter().skip(offset).take(batch_size) {
        db_oids.push(entry.database_oid as i64);
        db_names.push(entry.database_name.clone());
        owner_oids.push(entry.owner_oid as i64);
        encodings.push(entry.encoding);
        collates.push(entry.collate.clone());
        ctypes.push(entry.ctype.clone());
        is_templates.push(entry.is_template);
        allow_conns.push(entry.allow_conn);
        conn_limits.push(entry.conn_limit);
        acls.push(entry.acl.as_deref());
        count += 1;
    }

    // Update offset
    gstate.offset.fetch_add(count, Ordering::Relaxed);

    // Set column values
    if count > 0 {
        // Column 0: database_oid (BIGINT)
        let db_oid_vec = Vector::try_from_i64(&db_oids, output_allocator.clone())?;
        if let Some(col) = output.column_mut(0) {
            *col = db_oid_vec;
        }

        // Column 1: database_name (VARCHAR)
        let db_name_refs: Vec<&str> = db_names.iter().map(|s| s.as_str()).collect();
        let db_name_vec = Vector::try_from_strings(&db_name_refs, output_allocator.clone())?;
        if let Some(col) = output.column_mut(1) {
            *col = db_name_vec;
        }

        // Column 2: owner_oid (BIGINT)
        let owner_oid_vec = Vector::try_from_i64(&owner_oids, output_allocator.clone())?;
        if let Some(col) = output.column_mut(2) {
            *col = owner_oid_vec;
        }

        // Column 3: encoding (INTEGER)
        let encoding_vec = Vector::try_from_i32(&encodings, output_allocator.clone())?;
        if let Some(col) = output.column_mut(3) {
            *col = encoding_vec;
        }

        // Column 4: collate (VARCHAR)
        let collate_refs: Vec<&str> = collates.iter().map(|s| s.as_str()).collect();
        let collate_vec = Vector::try_from_strings(&collate_refs, output_allocator.clone())?;
        if let Some(col) = output.column_mut(4) {
            *col = collate_vec;
        }

        // Column 5: ctype (VARCHAR)
        let ctype_refs: Vec<&str> = ctypes.iter().map(|s| s.as_str()).collect();
        let ctype_vec = Vector::try_from_strings(&ctype_refs, output_allocator.clone())?;
        if let Some(col) = output.column_mut(5) {
            *col = ctype_vec;
        }

        // Column 6: is_template (BOOLEAN)
        let is_template_vec = Vector::try_from_bool(&is_templates, output_allocator.clone())?;
        if let Some(col) = output.column_mut(6) {
            *col = is_template_vec;
        }

        // Column 7: allow_conn (BOOLEAN)
        let allow_conn_vec = Vector::try_from_bool(&allow_conns, output_allocator.clone())?;
        if let Some(col) = output.column_mut(7) {
            *col = allow_conn_vec;
        }

        // Column 8: conn_limit (INTEGER)
        let conn_limit_vec = Vector::try_from_i32(&conn_limits, output_allocator.clone())?;
        if let Some(col) = output.column_mut(8) {
            *col = conn_limit_vec;
        }

        // Column 9: acl (VARCHAR, nullable)
        let acl_vec = Vector::try_from_nullable_strings(&acls, output_allocator.clone())?;
        if let Some(col) = output.column_mut(9) {
            *col = acl_vec;
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

/// Progress function for paro_databases().
fn paro_databases_progress(
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

/// Create the paro_databases() table function set.
pub fn create_paro_databases_function_set() -> TableFunctionSet {
    let mut func = TableFunction::new("paro_databases", vec![]);

    func.bind = Some(paro_databases_bind);
    func.init_global = Some(paro_databases_init_global);
    func.function = Some(paro_databases_function);
    func.table_scan_progress = Some(paro_databases_progress);

    let mut set = TableFunctionSet::new("paro_databases");
    set.add_function(func);
    set
}

/// Populate database data into the global state.
///
/// This is called by the executor when it has access to the DatabaseRegistry.
/// The executor should call this after creating the global state.
pub fn populate_database_data(state: &mut ParoDatabasesGlobalState, databases: Vec<DatabaseData>) {
    state.entries = databases;
    state.offset.store(0, Ordering::Relaxed);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::table::TableFunctionInitInput;
    use paro_common::runtime_value::Value;

    #[test]
    fn test_paro_databases_bind() {
        let empty_map = std::collections::HashMap::new();
        let input = TableFunctionBindInput::new(&[], &empty_map);
        let mut return_types = Vec::new();
        let mut names = Vec::new();

        let result = paro_databases_bind(&input, &mut return_types, &mut names);
        assert!(result.is_ok());

        // Verify columns
        assert_eq!(names.len(), 10);
        assert_eq!(names[0], "database_oid");
        assert_eq!(names[1], "database_name");
        assert_eq!(names[2], "owner_oid");
        assert_eq!(names[3], "encoding");
        assert_eq!(names[4], "collate");
        assert_eq!(names[5], "ctype");
        assert_eq!(names[6], "is_template");
        assert_eq!(names[7], "allow_conn");
        assert_eq!(names[8], "conn_limit");
        assert_eq!(names[9], "acl");

        assert_eq!(return_types.len(), 10);
        assert_eq!(return_types[0], LogicalType::BigInt);
        assert_eq!(return_types[1], LogicalType::Varchar);
        assert_eq!(return_types[2], LogicalType::BigInt);
        assert_eq!(return_types[3], LogicalType::Integer);
        assert_eq!(return_types[4], LogicalType::Varchar);
        assert_eq!(return_types[5], LogicalType::Varchar);
        assert_eq!(return_types[6], LogicalType::Boolean);
        assert_eq!(return_types[7], LogicalType::Boolean);
        assert_eq!(return_types[8], LogicalType::Integer);
        assert_eq!(return_types[9], LogicalType::Varchar);
    }

    #[test]
    fn test_paro_databases_init_global() {
        let input = TableFunctionInitInput::new_for_test(None, &[]);
        let result = paro_databases_init_global(&input);
        assert!(result.is_ok());

        let state = result.unwrap();
        assert!(state.is_some());
    }

    #[test]
    fn test_paro_databases_function_empty() {
        let input = TableFunctionInitInput::new_for_test(None, &[]);
        let state_box = paro_databases_init_global(&input).unwrap().unwrap();
        let state = state_box
            .as_any()
            .downcast_ref::<ParoDatabasesGlobalState>()
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
                LogicalType::Integer,
                LogicalType::Varchar,
                LogicalType::Varchar,
                LogicalType::Boolean,
                LogicalType::Boolean,
                LogicalType::Integer,
                LogicalType::Varchar,
            ],
            2048,
        );

        let result = paro_databases_function(&mut func_input, &mut chunk);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), TableFunctionResult::Finished);
        assert_eq!(chunk.size(), 0);
    }

    #[test]
    fn test_paro_databases_function_with_data() {
        let input = TableFunctionInitInput::new_for_test(None, &[]);
        let mut state_box = paro_databases_init_global(&input).unwrap().unwrap();

        // Populate with test data
        let state = state_box
            .as_any_mut()
            .downcast_mut::<ParoDatabasesGlobalState>()
            .unwrap();

        populate_database_data(
            state,
            vec![
                DatabaseData {
                    database_oid: 1,
                    database_name: "paro".to_string(),
                    owner_oid: 0,
                    encoding: 6,
                    collate: "C".to_string(),
                    ctype: "C".to_string(),
                    is_template: false,
                    allow_conn: true,
                    conn_limit: -1,
                    acl: None,
                },
                DatabaseData {
                    database_oid: 2,
                    database_name: "test_db".to_string(),
                    owner_oid: 0,
                    encoding: 6,
                    collate: "en_US.UTF-8".to_string(),
                    ctype: "en_US.UTF-8".to_string(),
                    is_template: false,
                    allow_conn: true,
                    conn_limit: 100,
                    acl: Some("user=rw".to_string()),
                },
            ],
        );

        let state_ref = state_box
            .as_any()
            .downcast_ref::<ParoDatabasesGlobalState>()
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
                LogicalType::Integer,
                LogicalType::Varchar,
                LogicalType::Varchar,
                LogicalType::Boolean,
                LogicalType::Boolean,
                LogicalType::Integer,
                LogicalType::Varchar,
            ],
            2048,
        );

        let result = paro_databases_function(&mut func_input, &mut chunk);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), TableFunctionResult::Finished);
        assert_eq!(chunk.size(), 2);

        // Verify data
        let db_oid_col = chunk.column(0).unwrap();
        assert_eq!(db_oid_col.get_value(0), Value::BigInt(1));
        assert_eq!(db_oid_col.get_value(1), Value::BigInt(2));

        let db_name_col = chunk.column(1).unwrap();
        assert_eq!(db_name_col.get_value(0), Value::Varchar("paro".to_string()));
        assert_eq!(
            db_name_col.get_value(1),
            Value::Varchar("test_db".to_string())
        );

        let allow_conn_col = chunk.column(7).unwrap();
        assert_eq!(allow_conn_col.get_value(0), Value::Boolean(true));
        assert_eq!(allow_conn_col.get_value(1), Value::Boolean(true));

        let conn_limit_col = chunk.column(8).unwrap();
        assert_eq!(conn_limit_col.get_value(0), Value::Integer(-1));
        assert_eq!(conn_limit_col.get_value(1), Value::Integer(100));
    }

    #[test]
    fn test_paro_databases_progress() {
        let input = TableFunctionInitInput::new_for_test(None, &[]);
        let mut state_box = paro_databases_init_global(&input).unwrap().unwrap();

        // Empty state should show 100% progress
        let state = state_box
            .as_any()
            .downcast_ref::<ParoDatabasesGlobalState>()
            .unwrap();
        assert!((paro_databases_progress(None, Some(state)) - 100.0).abs() < 0.001);

        // Add data and check progress
        let state_mut = state_box
            .as_any_mut()
            .downcast_mut::<ParoDatabasesGlobalState>()
            .unwrap();
        populate_database_data(
            state_mut,
            vec![
                DatabaseData {
                    database_oid: 1,
                    database_name: "db1".to_string(),
                    ..Default::default()
                },
                DatabaseData {
                    database_oid: 2,
                    database_name: "db2".to_string(),
                    ..Default::default()
                },
            ],
        );

        let state = state_box
            .as_any()
            .downcast_ref::<ParoDatabasesGlobalState>()
            .unwrap();

        // 0% progress at start
        assert!((paro_databases_progress(None, Some(state)) - 0.0).abs() < 0.001);

        // Advance offset
        state.offset.store(1, Ordering::Relaxed);
        assert!((paro_databases_progress(None, Some(state)) - 50.0).abs() < 0.001);

        // Complete
        state.offset.store(2, Ordering::Relaxed);
        assert!((paro_databases_progress(None, Some(state)) - 100.0).abs() < 0.001);
    }

    #[test]
    fn test_create_paro_databases_function_set() {
        let set = create_paro_databases_function_set();
        assert_eq!(set.name, "paro_databases");
        assert_eq!(set.functions.len(), 1);

        let func = &set.functions[0];
        assert_eq!(func.name, "paro_databases");
        assert!(func.arguments.is_empty());
        assert!(func.bind.is_some());
        assert!(func.init_global.is_some());
        assert!(func.function.is_some());
        assert!(func.table_scan_progress.is_some());
    }

    #[test]
    fn test_database_data_default() {
        let data = DatabaseData::default();
        assert_eq!(data.database_oid, 0);
        assert_eq!(data.database_name, "");
        assert_eq!(data.owner_oid, 0);
        assert_eq!(data.encoding, 6); // UTF8
        assert_eq!(data.collate, "C");
        assert_eq!(data.ctype, "C");
        assert!(!data.is_template);
        assert!(data.allow_conn);
        assert_eq!(data.conn_limit, -1);
        assert!(data.acl.is_none());
    }
}
