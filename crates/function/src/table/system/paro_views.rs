//! paro_views() Table Function
//!
//!
//!
//! ## Overview
//! Returns information about all views in the database.
//!
//! ## Return Columns
//!
//! ## Example
//! ```sql
//! SELECT * FROM paro_views();
//! SELECT view_name FROM paro_views() WHERE NOT internal;
//! SELECT * FROM paro_views() WHERE schema_name = 'public';
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

/// Bind data for paro_views().
///
/// This is empty since paro_views() takes no arguments.
/// The actual view data is collected at init time.
#[derive(Clone)]
pub struct ParoViewsBindData;

impl TableFunctionBindData for ParoViewsBindData {
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

/// View entry data collected from the catalog.
#[derive(Debug, Clone)]
pub struct ViewData {
    /// Database name
    pub database_name: String,
    /// Database OID
    pub database_oid: u64,
    /// Schema name
    pub schema_name: String,
    /// Schema OID
    pub schema_oid: u64,
    /// View name
    pub view_name: String,
    /// View OID
    pub view_oid: u64,
    /// Whether this is an internal view
    pub internal: bool,
    /// Whether this is a temporary view
    pub temporary: bool,
    /// Number of columns
    pub column_count: i64,
    /// Original SQL statement
    pub sql: Option<String>,
}

/// Global state for paro_views().
///
/// Contains the collected view data and current offset.
pub struct ParoViewsGlobalState {
    /// Collected view entries
    pub entries: Vec<ViewData>,
    /// Current offset into entries
    pub offset: AtomicUsize,
}

impl GlobalTableFunctionState for ParoViewsGlobalState {
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

/// Bind function for paro_views().
fn paro_views_bind(
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

    names.push("view_name".to_string());
    return_types.push(LogicalType::Varchar);

    names.push("view_oid".to_string());
    return_types.push(LogicalType::BigInt);

    names.push("internal".to_string());
    return_types.push(LogicalType::Boolean);

    names.push("temporary".to_string());
    return_types.push(LogicalType::Boolean);

    names.push("column_count".to_string());
    return_types.push(LogicalType::BigInt);

    names.push("sql".to_string());
    return_types.push(LogicalType::Varchar);

    Ok(Some(Box::new(ParoViewsBindData)))
}

/// Init global function for paro_views().
///
/// Note: This function cannot access the catalog directly because
/// table functions don't have access to the execution context at init time.
/// The view data must be injected via a different mechanism.
///
/// For now, we return an empty state. The actual view data will be
/// populated by the executor when it has access to the catalog.
fn paro_views_init_global(
    _input: &TableFunctionInitInput,
) -> Result<Option<Box<dyn GlobalTableFunctionState>>> {
    // Create empty state - will be populated by executor
    Ok(Some(Box::new(ParoViewsGlobalState {
        entries: Vec::new(),
        offset: AtomicUsize::new(0),
    })))
}

/// Main function for paro_views().
fn paro_views_function(
    input: &mut TableFunctionInput,
    output: &mut Chunk,
) -> Result<TableFunctionResult> {
    let gstate = input
        .global_state
        .and_then(|gs| gs.as_any().downcast_ref::<ParoViewsGlobalState>());

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
    let mut view_names = Vec::with_capacity(batch_size);
    let mut view_oids = Vec::with_capacity(batch_size);
    let mut internals = Vec::with_capacity(batch_size);
    let mut temporaries = Vec::with_capacity(batch_size);
    let mut column_counts = Vec::with_capacity(batch_size);
    let mut sqls: Vec<Option<String>> = Vec::with_capacity(batch_size);

    for entry in gstate.entries.iter().skip(offset).take(batch_size) {
        db_names.push(entry.database_name.clone());
        db_oids.push(entry.database_oid as i64);
        schema_names.push(entry.schema_name.clone());
        schema_oids.push(entry.schema_oid as i64);
        view_names.push(entry.view_name.clone());
        view_oids.push(entry.view_oid as i64);
        internals.push(entry.internal);
        temporaries.push(entry.temporary);
        column_counts.push(entry.column_count);
        sqls.push(entry.sql.clone());
        count += 1;
    }

    // Update offset
    gstate.offset.fetch_add(count, Ordering::Relaxed);

    // Set column values
    if count > 0 {
        // Column 0: database_name (VARCHAR)
        let db_name_refs: Vec<&str> = db_names.iter().map(|s| s.as_str()).collect();
        let db_name_vec = Vector::from_strings(&db_name_refs);
        if let Some(col) = output.column_mut(0) {
            *col = db_name_vec;
        }

        // Column 1: database_oid (BIGINT)
        let db_oid_vec = Vector::from_i64(&db_oids);
        if let Some(col) = output.column_mut(1) {
            *col = db_oid_vec;
        }

        // Column 2: schema_name (VARCHAR)
        let schema_name_refs: Vec<&str> = schema_names.iter().map(|s| s.as_str()).collect();
        let schema_name_vec = Vector::from_strings(&schema_name_refs);
        if let Some(col) = output.column_mut(2) {
            *col = schema_name_vec;
        }

        // Column 3: schema_oid (BIGINT)
        let schema_oid_vec = Vector::from_i64(&schema_oids);
        if let Some(col) = output.column_mut(3) {
            *col = schema_oid_vec;
        }

        // Column 4: view_name (VARCHAR)
        let view_name_refs: Vec<&str> = view_names.iter().map(|s| s.as_str()).collect();
        let view_name_vec = Vector::from_strings(&view_name_refs);
        if let Some(col) = output.column_mut(4) {
            *col = view_name_vec;
        }

        // Column 5: view_oid (BIGINT)
        let view_oid_vec = Vector::from_i64(&view_oids);
        if let Some(col) = output.column_mut(5) {
            *col = view_oid_vec;
        }

        // Column 6: internal (BOOLEAN)
        let internal_vec = Vector::from_bool(&internals);
        if let Some(col) = output.column_mut(6) {
            *col = internal_vec;
        }

        // Column 7: temporary (BOOLEAN)
        let temporary_vec = Vector::from_bool(&temporaries);
        if let Some(col) = output.column_mut(7) {
            *col = temporary_vec;
        }

        // Column 8: column_count (BIGINT)
        let column_count_vec = Vector::from_i64(&column_counts);
        if let Some(col) = output.column_mut(8) {
            *col = column_count_vec;
        }

        // Column 9: sql (VARCHAR) - handle NULL values
        let sql_refs: Vec<Option<&str>> = sqls.iter().map(|s| s.as_deref()).collect();
        let sql_vec = Vector::from_nullable_strings(&sql_refs);
        if let Some(col) = output.column_mut(9) {
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

/// Progress function for paro_views().
fn paro_views_progress(
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

/// Create the paro_views() table function set.
pub fn create_paro_views_function_set() -> TableFunctionSet {
    let mut func = TableFunction::new("paro_views", vec![]);

    func.bind = Some(paro_views_bind);
    func.init_global = Some(paro_views_init_global);
    func.function = Some(paro_views_function);
    func.table_scan_progress = Some(paro_views_progress);

    let mut set = TableFunctionSet::new("paro_views");
    set.add_function(func);
    set
}

/// Populate view data into the global state.
///
/// This is called by the executor when it has access to the catalog.
/// The executor should call this after creating the global state.
pub fn populate_view_data(state: &mut ParoViewsGlobalState, views: Vec<ViewData>) {
    state.entries = views;
    state.offset.store(0, Ordering::Relaxed);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::table::TableFunctionInitInput;

    #[test]
    fn test_paro_views_bind() {
        let empty_map = std::collections::HashMap::new();
        let input = TableFunctionBindInput::new(&[], &empty_map);
        let mut return_types = Vec::new();
        let mut names = Vec::new();

        let result = paro_views_bind(&input, &mut return_types, &mut names);
        assert!(result.is_ok());

        // Verify columns
        assert_eq!(names.len(), 10);
        assert_eq!(names[0], "database_name");
        assert_eq!(names[1], "database_oid");
        assert_eq!(names[2], "schema_name");
        assert_eq!(names[3], "schema_oid");
        assert_eq!(names[4], "view_name");
        assert_eq!(names[5], "view_oid");
        assert_eq!(names[6], "internal");
        assert_eq!(names[7], "temporary");
        assert_eq!(names[8], "column_count");
        assert_eq!(names[9], "sql");

        assert_eq!(return_types.len(), 10);
        assert_eq!(return_types[0], LogicalType::Varchar);
        assert_eq!(return_types[1], LogicalType::BigInt);
        assert_eq!(return_types[2], LogicalType::Varchar);
        assert_eq!(return_types[3], LogicalType::BigInt);
        assert_eq!(return_types[4], LogicalType::Varchar);
        assert_eq!(return_types[5], LogicalType::BigInt);
        assert_eq!(return_types[6], LogicalType::Boolean);
        assert_eq!(return_types[7], LogicalType::Boolean);
        assert_eq!(return_types[8], LogicalType::BigInt);
        assert_eq!(return_types[9], LogicalType::Varchar);
    }

    #[test]
    fn test_paro_views_init_global() {
        let input = TableFunctionInitInput::new(None, &[]);
        let result = paro_views_init_global(&input);
        assert!(result.is_ok());

        let state = result.unwrap();
        assert!(state.is_some());
    }

    #[test]
    fn test_paro_views_function_empty() {
        let input = TableFunctionInitInput::new(None, &[]);
        let state_box = paro_views_init_global(&input).unwrap().unwrap();
        let state = state_box
            .as_any()
            .downcast_ref::<ParoViewsGlobalState>()
            .unwrap();

        // Empty state should return finished immediately
        let mut func_input = TableFunctionInput {
            bind_data: None,
            local_state: None,
            global_state: Some(state),
        };

        let mut chunk = Chunk::initialize(
            &[
                LogicalType::Varchar, // database_name
                LogicalType::BigInt,  // database_oid
                LogicalType::Varchar, // schema_name
                LogicalType::BigInt,  // schema_oid
                LogicalType::Varchar, // view_name
                LogicalType::BigInt,  // view_oid
                LogicalType::Boolean, // internal
                LogicalType::Boolean, // temporary
                LogicalType::BigInt,  // column_count
                LogicalType::Varchar, // sql
            ],
            2048,
        );

        let result = paro_views_function(&mut func_input, &mut chunk);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), TableFunctionResult::Finished);
        assert_eq!(chunk.size(), 0);
    }

    #[test]
    fn test_paro_views_function_with_data() {
        use paro_common::runtime_value::Value;

        let input = TableFunctionInitInput::new(None, &[]);
        let mut state_box = paro_views_init_global(&input).unwrap().unwrap();

        // Populate with test data
        let state = state_box
            .as_any_mut()
            .downcast_mut::<ParoViewsGlobalState>()
            .unwrap();

        populate_view_data(
            state,
            vec![
                ViewData {
                    database_name: "test_db".to_string(),
                    database_oid: 1,
                    schema_name: "public".to_string(),
                    schema_oid: 10,
                    view_name: "active_users".to_string(),
                    view_oid: 100,
                    internal: false,
                    temporary: false,
                    column_count: 3,
                    sql: Some(
                        "CREATE VIEW active_users AS SELECT * FROM users WHERE active".to_string(),
                    ),
                },
                ViewData {
                    database_name: "test_db".to_string(),
                    database_oid: 1,
                    schema_name: "public".to_string(),
                    schema_oid: 10,
                    view_name: "recent_orders".to_string(),
                    view_oid: 101,
                    internal: false,
                    temporary: true,
                    column_count: 5,
                    sql: None,
                },
                ViewData {
                    database_name: "test_db".to_string(),
                    database_oid: 1,
                    schema_name: "pg_catalog".to_string(),
                    schema_oid: 11,
                    view_name: "pg_views".to_string(),
                    view_oid: 102,
                    internal: true,
                    temporary: false,
                    column_count: 4,
                    sql: Some("CREATE VIEW pg_views AS SELECT...".to_string()),
                },
            ],
        );

        let state_ref = state_box
            .as_any()
            .downcast_ref::<ParoViewsGlobalState>()
            .unwrap();

        let mut func_input = TableFunctionInput {
            bind_data: None,
            local_state: None,
            global_state: Some(state_ref),
        };

        let mut chunk = Chunk::initialize(
            &[
                LogicalType::Varchar, // database_name
                LogicalType::BigInt,  // database_oid
                LogicalType::Varchar, // schema_name
                LogicalType::BigInt,  // schema_oid
                LogicalType::Varchar, // view_name
                LogicalType::BigInt,  // view_oid
                LogicalType::Boolean, // internal
                LogicalType::Boolean, // temporary
                LogicalType::BigInt,  // column_count
                LogicalType::Varchar, // sql
            ],
            2048,
        );

        let result = paro_views_function(&mut func_input, &mut chunk);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), TableFunctionResult::Finished);
        assert_eq!(chunk.size(), 3);

        // Verify data
        let view_name_col = chunk.column(4).unwrap();
        assert_eq!(
            view_name_col.get_value(0),
            Value::Varchar("active_users".to_string())
        );
        assert_eq!(
            view_name_col.get_value(1),
            Value::Varchar("recent_orders".to_string())
        );
        assert_eq!(
            view_name_col.get_value(2),
            Value::Varchar("pg_views".to_string())
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
        assert_eq!(column_count_col.get_value(0), Value::BigInt(3));
        assert_eq!(column_count_col.get_value(1), Value::BigInt(5));
        assert_eq!(column_count_col.get_value(2), Value::BigInt(4));
    }

    #[test]
    fn test_paro_views_progress() {
        let input = TableFunctionInitInput::new(None, &[]);
        let mut state_box = paro_views_init_global(&input).unwrap().unwrap();

        // Empty state should show 100% progress
        let state = state_box
            .as_any()
            .downcast_ref::<ParoViewsGlobalState>()
            .unwrap();
        assert!((paro_views_progress(None, Some(state)) - 100.0).abs() < 0.001);

        // Add data and check progress
        let state_mut = state_box
            .as_any_mut()
            .downcast_mut::<ParoViewsGlobalState>()
            .unwrap();
        populate_view_data(
            state_mut,
            vec![
                ViewData {
                    database_name: "test".to_string(),
                    database_oid: 1,
                    schema_name: "public".to_string(),
                    schema_oid: 10,
                    view_name: "v1".to_string(),
                    view_oid: 100,
                    internal: false,
                    temporary: false,
                    column_count: 2,
                    sql: None,
                },
                ViewData {
                    database_name: "test".to_string(),
                    database_oid: 1,
                    schema_name: "public".to_string(),
                    schema_oid: 10,
                    view_name: "v2".to_string(),
                    view_oid: 101,
                    internal: false,
                    temporary: false,
                    column_count: 3,
                    sql: None,
                },
            ],
        );

        let state = state_box
            .as_any()
            .downcast_ref::<ParoViewsGlobalState>()
            .unwrap();

        // 0% progress at start
        assert!((paro_views_progress(None, Some(state)) - 0.0).abs() < 0.001);

        // Advance offset
        state.offset.store(1, Ordering::Relaxed);
        assert!((paro_views_progress(None, Some(state)) - 50.0).abs() < 0.001);

        // Complete
        state.offset.store(2, Ordering::Relaxed);
        assert!((paro_views_progress(None, Some(state)) - 100.0).abs() < 0.001);
    }

    #[test]
    fn test_create_paro_views_function_set() {
        let set = create_paro_views_function_set();
        assert_eq!(set.name, "paro_views");
        assert_eq!(set.functions.len(), 1);

        let func = &set.functions[0];
        assert_eq!(func.name, "paro_views");
        assert!(func.arguments.is_empty());
        assert!(func.bind.is_some());
        assert!(func.init_global.is_some());
        assert!(func.function.is_some());
        assert!(func.table_scan_progress.is_some());
    }

    #[test]
    fn test_paro_views_large_batch() {
        let input = TableFunctionInitInput::new(None, &[]);
        let mut state_box = paro_views_init_global(&input).unwrap().unwrap();

        // Create many views to test batching
        let state = state_box
            .as_any_mut()
            .downcast_mut::<ParoViewsGlobalState>()
            .unwrap();

        let mut views = Vec::new();
        for i in 0..3000 {
            views.push(ViewData {
                database_name: "test_db".to_string(),
                database_oid: 1,
                schema_name: "public".to_string(),
                schema_oid: 10,
                view_name: format!("view_{}", i),
                view_oid: 100 + i as u64,
                internal: false,
                temporary: false,
                column_count: 3,
                sql: Some(format!("CREATE VIEW view_{} AS SELECT {}", i, i)),
            });
        }
        populate_view_data(state, views);

        let state_ref = state_box
            .as_any()
            .downcast_ref::<ParoViewsGlobalState>()
            .unwrap();

        let mut func_input = TableFunctionInput {
            bind_data: None,
            local_state: None,
            global_state: Some(state_ref),
        };

        let mut chunk = Chunk::initialize(
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
                LogicalType::Varchar,
            ],
            2048,
        );

        // First batch should return 2048 rows
        let result = paro_views_function(&mut func_input, &mut chunk);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), TableFunctionResult::HaveMoreOutput);
        assert_eq!(chunk.size(), 2048);

        // Second batch should return remaining 952 rows
        let result = paro_views_function(&mut func_input, &mut chunk);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), TableFunctionResult::Finished);
        assert_eq!(chunk.size(), 952);
    }
}
