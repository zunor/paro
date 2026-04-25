// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! paro_columns() Table Function
//!
//!
//!
//! ## Overview
//! Returns information about all columns in all tables in the database.
//!
//! ## Return Columns
//!
//! ## Example
//! ```sql
//! SELECT * FROM paro_columns();
//! SELECT column_name, data_type FROM paro_columns() WHERE table_name = 'users';
//! SELECT * FROM paro_columns() WHERE NOT internal;
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

/// Bind data for paro_columns().
///
/// This is empty since paro_columns() takes no arguments.
/// The actual column data is collected at init time.
#[derive(Clone)]
pub struct ParoColumnsBindData;

impl TableFunctionBindData for ParoColumnsBindData {
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

/// Column entry data collected from the catalog.
#[derive(Debug, Clone)]
pub struct ColumnData {
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
    /// Column name
    pub column_name: String,
    /// Column index (1-based)
    pub column_index: i32,
    /// Whether this is an internal table
    pub internal: bool,
    /// Whether the column is nullable
    pub is_nullable: bool,
    /// Data type name
    pub data_type: String,
    /// Data type ID
    pub data_type_id: u64,
    /// Column default value
    pub column_default: Option<String>,
    /// Character maximum length
    pub character_maximum_length: Option<u64>,
    /// Character octet length
    pub character_octet_length: Option<u64>,
    /// Numeric precision
    pub numeric_precision: Option<u64>,
    /// Numeric precision radix
    pub numeric_precision_radix: Option<u64>,
    /// Numeric scale
    pub numeric_scale: Option<u64>,
    /// Datetime precision
    pub datetime_precision: Option<u64>,
}

/// Global state for paro_columns().
///
/// Contains the collected column data and current offset.
pub struct ParoColumnsGlobalState {
    /// Collected column entries
    pub entries: Vec<ColumnData>,
    /// Current offset into entries
    pub offset: AtomicUsize,
}

impl GlobalTableFunctionState for ParoColumnsGlobalState {
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

/// Bind function for paro_columns().
fn paro_columns_bind(
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

    names.push("column_name".to_string());
    return_types.push(LogicalType::Varchar);

    names.push("column_index".to_string());
    return_types.push(LogicalType::Integer);

    names.push("internal".to_string());
    return_types.push(LogicalType::Boolean);

    names.push("is_nullable".to_string());
    return_types.push(LogicalType::Boolean);

    names.push("data_type".to_string());
    return_types.push(LogicalType::Varchar);

    names.push("data_type_id".to_string());
    return_types.push(LogicalType::BigInt);

    names.push("column_default".to_string());
    return_types.push(LogicalType::Varchar);

    names.push("character_maximum_length".to_string());
    return_types.push(LogicalType::BigInt);

    names.push("character_octet_length".to_string());
    return_types.push(LogicalType::BigInt);

    names.push("numeric_precision".to_string());
    return_types.push(LogicalType::BigInt);

    names.push("numeric_precision_radix".to_string());
    return_types.push(LogicalType::BigInt);

    names.push("numeric_scale".to_string());
    return_types.push(LogicalType::BigInt);

    names.push("datetime_precision".to_string());
    return_types.push(LogicalType::BigInt);

    Ok(Some(Box::new(ParoColumnsBindData)))
}

/// Init global function for paro_columns().
///
/// Note: This function cannot access the catalog directly because
/// table functions don't have access to the execution context at init time.
/// The column data must be injected via a different mechanism.
///
/// For now, we return an empty state. The actual column data will be
/// populated by the executor when it has access to the catalog.
fn paro_columns_init_global(
    _input: &TableFunctionInitInput,
) -> Result<Option<Box<dyn GlobalTableFunctionState>>> {
    // Create empty state - will be populated by executor
    Ok(Some(Box::new(ParoColumnsGlobalState {
        entries: Vec::new(),
        offset: AtomicUsize::new(0),
    })))
}

/// Main function for paro_columns().
fn paro_columns_function(
    input: &mut TableFunctionInput,
    output: &mut Chunk,
) -> Result<TableFunctionResult> {
    let output_allocator = output.allocator().clone();
    let gstate = input
        .global_state
        .and_then(|gs| gs.as_any().downcast_ref::<ParoColumnsGlobalState>());

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
    let mut column_names = Vec::with_capacity(batch_size);
    let mut column_indexes = Vec::with_capacity(batch_size);
    let mut internals = Vec::with_capacity(batch_size);
    let mut is_nullables = Vec::with_capacity(batch_size);
    let mut data_types = Vec::with_capacity(batch_size);
    let mut data_type_ids = Vec::with_capacity(batch_size);
    let mut column_defaults = Vec::with_capacity(batch_size);
    let mut character_maximum_lengths = Vec::with_capacity(batch_size);
    let mut character_octet_lengths = Vec::with_capacity(batch_size);
    let mut numeric_precisions = Vec::with_capacity(batch_size);
    let mut numeric_precision_radixes = Vec::with_capacity(batch_size);
    let mut numeric_scales = Vec::with_capacity(batch_size);
    let mut datetime_precisions = Vec::with_capacity(batch_size);

    for entry in gstate.entries.iter().skip(offset).take(batch_size) {
        db_names.push(entry.database_name.clone());
        db_oids.push(entry.database_oid as i64);
        schema_names.push(entry.schema_name.clone());
        schema_oids.push(entry.schema_oid as i64);
        table_names.push(entry.table_name.clone());
        table_oids.push(entry.table_oid as i64);
        column_names.push(entry.column_name.clone());
        column_indexes.push(entry.column_index);
        internals.push(entry.internal);
        is_nullables.push(entry.is_nullable);
        data_types.push(entry.data_type.clone());
        data_type_ids.push(entry.data_type_id as i64);
        column_defaults.push(entry.column_default.clone());
        character_maximum_lengths.push(entry.character_maximum_length);
        character_octet_lengths.push(entry.character_octet_length);
        numeric_precisions.push(entry.numeric_precision);
        numeric_precision_radixes.push(entry.numeric_precision_radix);
        numeric_scales.push(entry.numeric_scale);
        datetime_precisions.push(entry.datetime_precision);
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

        // Column 6: column_name (VARCHAR)
        let column_name_refs: Vec<&str> = column_names.iter().map(|s| s.as_str()).collect();
        let column_name_vec =
            Vector::try_from_strings(&column_name_refs, output_allocator.clone())?;
        if let Some(col) = output.column_mut(6) {
            *col = column_name_vec;
        }

        // Column 7: column_index (INTEGER)
        let column_index_vec = Vector::try_from_i32(&column_indexes, output_allocator.clone())?;
        if let Some(col) = output.column_mut(7) {
            *col = column_index_vec;
        }

        // Column 8: internal (BOOLEAN)
        let internal_vec = Vector::try_from_bool(&internals, output_allocator.clone())?;
        if let Some(col) = output.column_mut(8) {
            *col = internal_vec;
        }

        // Column 9: is_nullable (BOOLEAN)
        let is_nullable_vec = Vector::try_from_bool(&is_nullables, output_allocator.clone())?;
        if let Some(col) = output.column_mut(9) {
            *col = is_nullable_vec;
        }

        // Column 10: data_type (VARCHAR)
        let data_type_refs: Vec<&str> = data_types.iter().map(|s| s.as_str()).collect();
        let data_type_vec = Vector::try_from_strings(&data_type_refs, output_allocator.clone())?;
        if let Some(col) = output.column_mut(10) {
            *col = data_type_vec;
        }

        // Column 11: data_type_id (BIGINT)
        let data_type_id_vec = Vector::try_from_i64(&data_type_ids, output_allocator.clone())?;
        if let Some(col) = output.column_mut(11) {
            *col = data_type_id_vec;
        }

        // Column 12: column_default (VARCHAR)
        let column_default_refs: Vec<Option<&str>> =
            column_defaults.iter().map(|s| s.as_deref()).collect();
        let column_default_vec =
            Vector::try_from_nullable_strings(&column_default_refs, output_allocator.clone())?;
        if let Some(col) = output.column_mut(12) {
            *col = column_default_vec;
        }

        // Column 13: character_maximum_length (BIGINT)
        let character_maximum_length_vec =
            Vector::try_from_nullable_u64(&character_maximum_lengths, output_allocator.clone())?;
        if let Some(col) = output.column_mut(13) {
            *col = character_maximum_length_vec;
        }

        // Column 14: character_octet_length (BIGINT)
        let character_octet_length_vec =
            Vector::try_from_nullable_u64(&character_octet_lengths, output_allocator.clone())?;
        if let Some(col) = output.column_mut(14) {
            *col = character_octet_length_vec;
        }

        // Column 15: numeric_precision (BIGINT)
        let numeric_precision_vec =
            Vector::try_from_nullable_u64(&numeric_precisions, output_allocator.clone())?;
        if let Some(col) = output.column_mut(15) {
            *col = numeric_precision_vec;
        }

        // Column 16: numeric_precision_radix (BIGINT)
        let numeric_precision_radix_vec =
            Vector::try_from_nullable_u64(&numeric_precision_radixes, output_allocator.clone())?;
        if let Some(col) = output.column_mut(16) {
            *col = numeric_precision_radix_vec;
        }

        // Column 17: numeric_scale (BIGINT)
        let numeric_scale_vec =
            Vector::try_from_nullable_u64(&numeric_scales, output_allocator.clone())?;
        if let Some(col) = output.column_mut(17) {
            *col = numeric_scale_vec;
        }

        // Column 18: datetime_precision (BIGINT)
        let datetime_precision_vec =
            Vector::try_from_nullable_u64(&datetime_precisions, output_allocator.clone())?;
        if let Some(col) = output.column_mut(18) {
            *col = datetime_precision_vec;
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

/// Progress function for paro_columns().
fn paro_columns_progress(
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

/// Create the paro_columns() table function set.
pub fn create_paro_columns_function_set() -> TableFunctionSet {
    let mut func = TableFunction::new("paro_columns", vec![]);

    func.bind = Some(paro_columns_bind);
    func.init_global = Some(paro_columns_init_global);
    func.function = Some(paro_columns_function);
    func.table_scan_progress = Some(paro_columns_progress);

    let mut set = TableFunctionSet::new("paro_columns");
    set.add_function(func);
    set
}

/// Populate column data into the global state.
///
/// This is called by the executor when it has access to the catalog.
/// The executor should call this after creating the global state.
pub fn populate_column_data(state: &mut ParoColumnsGlobalState, columns: Vec<ColumnData>) {
    state.entries = columns;
    state.offset.store(0, Ordering::Relaxed);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::table::TableFunctionInitInput;
    use paro_common::runtime_value::Value;

    #[test]
    fn test_paro_columns_bind() {
        let empty_map = std::collections::HashMap::new();
        let input = TableFunctionBindInput::new(&[], &empty_map);
        let mut return_types = Vec::new();
        let mut names = Vec::new();

        let result = paro_columns_bind(&input, &mut return_types, &mut names);
        assert!(result.is_ok());

        // Verify columns (19 total)
        assert_eq!(names.len(), 19);
        assert_eq!(names[0], "database_name");
        assert_eq!(names[1], "database_oid");
        assert_eq!(names[2], "schema_name");
        assert_eq!(names[3], "schema_oid");
        assert_eq!(names[4], "table_name");
        assert_eq!(names[5], "table_oid");
        assert_eq!(names[6], "column_name");
        assert_eq!(names[7], "column_index");
        assert_eq!(names[8], "internal");
        assert_eq!(names[9], "is_nullable");
        assert_eq!(names[10], "data_type");
        assert_eq!(names[11], "data_type_id");
        assert_eq!(names[12], "column_default");
        assert_eq!(names[13], "character_maximum_length");
        assert_eq!(names[14], "character_octet_length");
        assert_eq!(names[15], "numeric_precision");
        assert_eq!(names[16], "numeric_precision_radix");
        assert_eq!(names[17], "numeric_scale");
        assert_eq!(names[18], "datetime_precision");

        assert_eq!(return_types.len(), 19);
        assert_eq!(return_types[0], LogicalType::Varchar);
        assert_eq!(return_types[1], LogicalType::BigInt);
        assert_eq!(return_types[2], LogicalType::Varchar);
        assert_eq!(return_types[3], LogicalType::BigInt);
        assert_eq!(return_types[4], LogicalType::Varchar);
        assert_eq!(return_types[5], LogicalType::BigInt);
        assert_eq!(return_types[6], LogicalType::Varchar);
        assert_eq!(return_types[7], LogicalType::Integer);
        assert_eq!(return_types[8], LogicalType::Boolean);
        assert_eq!(return_types[9], LogicalType::Boolean);
        assert_eq!(return_types[10], LogicalType::Varchar);
        assert_eq!(return_types[11], LogicalType::BigInt);
        assert_eq!(return_types[12], LogicalType::Varchar);
        assert_eq!(return_types[13], LogicalType::BigInt);
        assert_eq!(return_types[14], LogicalType::BigInt);
        assert_eq!(return_types[15], LogicalType::BigInt);
        assert_eq!(return_types[16], LogicalType::BigInt);
        assert_eq!(return_types[17], LogicalType::BigInt);
        assert_eq!(return_types[18], LogicalType::BigInt);
    }

    #[test]
    fn test_paro_columns_init_global() {
        let input = TableFunctionInitInput::new(None, &[]);
        let result = paro_columns_init_global(&input);
        assert!(result.is_ok());

        let state = result.unwrap();
        assert!(state.is_some());
    }

    #[test]
    fn test_paro_columns_function_empty() {
        let input = TableFunctionInitInput::new(None, &[]);
        let state_box = paro_columns_init_global(&input).unwrap().unwrap();
        let state = state_box
            .as_any()
            .downcast_ref::<ParoColumnsGlobalState>()
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
                LogicalType::Varchar, // column_name
                LogicalType::Integer, // column_index
                LogicalType::Boolean, // internal
                LogicalType::Boolean, // is_nullable
                LogicalType::Varchar, // data_type
                LogicalType::BigInt,  // data_type_id
                LogicalType::Varchar, // column_default
                LogicalType::BigInt,  // character_maximum_length
                LogicalType::BigInt,  // character_octet_length
                LogicalType::BigInt,  // numeric_precision
                LogicalType::BigInt,  // numeric_precision_radix
                LogicalType::BigInt,  // numeric_scale
                LogicalType::BigInt,  // datetime_precision
            ],
            2048,
        );

        let result = paro_columns_function(&mut func_input, &mut chunk);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), TableFunctionResult::Finished);
        assert_eq!(chunk.size(), 0);
    }

    #[test]
    fn test_paro_columns_function_with_data() {
        let input = TableFunctionInitInput::new(None, &[]);
        let mut state_box = paro_columns_init_global(&input).unwrap().unwrap();

        // Populate with test data
        let state = state_box
            .as_any_mut()
            .downcast_mut::<ParoColumnsGlobalState>()
            .unwrap();

        populate_column_data(
            state,
            vec![
                ColumnData {
                    database_name: "test_db".to_string(),
                    database_oid: 1,
                    schema_name: "public".to_string(),
                    schema_oid: 10,
                    table_name: "users".to_string(),
                    table_oid: 100,
                    column_name: "id".to_string(),
                    column_index: 1,
                    internal: false,
                    is_nullable: false,
                    data_type: "BIGINT".to_string(),
                    data_type_id: 2,
                    column_default: None,
                    character_maximum_length: None,
                    character_octet_length: None,
                    numeric_precision: None,
                    numeric_precision_radix: None,
                    numeric_scale: None,
                    datetime_precision: None,
                },
                ColumnData {
                    database_name: "test_db".to_string(),
                    database_oid: 1,
                    schema_name: "public".to_string(),
                    schema_oid: 10,
                    table_name: "users".to_string(),
                    table_oid: 100,
                    column_name: "name".to_string(),
                    column_index: 2,
                    internal: false,
                    is_nullable: true,
                    data_type: "VARCHAR".to_string(),
                    data_type_id: 17,
                    column_default: None,
                    character_maximum_length: None,
                    character_octet_length: None,
                    numeric_precision: None,
                    numeric_precision_radix: None,
                    numeric_scale: None,
                    datetime_precision: None,
                },
                ColumnData {
                    database_name: "test_db".to_string(),
                    database_oid: 1,
                    schema_name: "public".to_string(),
                    schema_oid: 10,
                    table_name: "users".to_string(),
                    table_oid: 100,
                    column_name: "email".to_string(),
                    column_index: 3,
                    internal: false,
                    is_nullable: true,
                    data_type: "VARCHAR".to_string(),
                    data_type_id: 17,
                    column_default: None,
                    character_maximum_length: None,
                    character_octet_length: None,
                    numeric_precision: None,
                    numeric_precision_radix: None,
                    numeric_scale: None,
                    datetime_precision: None,
                },
            ],
        );

        let state_ref = state_box
            .as_any()
            .downcast_ref::<ParoColumnsGlobalState>()
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
                LogicalType::Varchar,
                LogicalType::Integer,
                LogicalType::Boolean,
                LogicalType::Boolean,
                LogicalType::Varchar,
                LogicalType::BigInt,
                LogicalType::Varchar, // column_default
                LogicalType::BigInt,  // character_maximum_length
                LogicalType::BigInt,  // character_octet_length
                LogicalType::BigInt,  // numeric_precision
                LogicalType::BigInt,  // numeric_precision_radix
                LogicalType::BigInt,  // numeric_scale
                LogicalType::BigInt,  // datetime_precision
            ],
            2048,
        );

        let result = paro_columns_function(&mut func_input, &mut chunk);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), TableFunctionResult::Finished);
        assert_eq!(chunk.size(), 3);

        // Verify data
        let column_name_col = chunk.column(6).unwrap();
        assert_eq!(
            column_name_col.get_value(0),
            Value::Varchar("id".to_string())
        );
        assert_eq!(
            column_name_col.get_value(1),
            Value::Varchar("name".to_string())
        );
        assert_eq!(
            column_name_col.get_value(2),
            Value::Varchar("email".to_string())
        );

        let column_index_col = chunk.column(7).unwrap();
        assert_eq!(column_index_col.get_value(0), Value::Integer(1));
        assert_eq!(column_index_col.get_value(1), Value::Integer(2));
        assert_eq!(column_index_col.get_value(2), Value::Integer(3));

        let is_nullable_col = chunk.column(9).unwrap();
        assert_eq!(is_nullable_col.get_value(0), Value::Boolean(false));
        assert_eq!(is_nullable_col.get_value(1), Value::Boolean(true));
        assert_eq!(is_nullable_col.get_value(2), Value::Boolean(true));

        let data_type_col = chunk.column(10).unwrap();
        assert_eq!(
            data_type_col.get_value(0),
            Value::Varchar("BIGINT".to_string())
        );
        assert_eq!(
            data_type_col.get_value(1),
            Value::Varchar("VARCHAR".to_string())
        );
        assert_eq!(
            data_type_col.get_value(2),
            Value::Varchar("VARCHAR".to_string())
        );
    }

    #[test]
    fn test_paro_columns_progress() {
        let input = TableFunctionInitInput::new(None, &[]);
        let mut state_box = paro_columns_init_global(&input).unwrap().unwrap();

        // Empty state should show 100% progress
        let state = state_box
            .as_any()
            .downcast_ref::<ParoColumnsGlobalState>()
            .unwrap();
        assert!((paro_columns_progress(None, Some(state)) - 100.0).abs() < 0.001);

        // Add data and check progress
        let state_mut = state_box
            .as_any_mut()
            .downcast_mut::<ParoColumnsGlobalState>()
            .unwrap();
        populate_column_data(
            state_mut,
            vec![
                ColumnData {
                    database_name: "test".to_string(),
                    database_oid: 1,
                    schema_name: "public".to_string(),
                    schema_oid: 10,
                    table_name: "t1".to_string(),
                    table_oid: 100,
                    column_name: "c1".to_string(),
                    column_index: 1,
                    internal: false,
                    is_nullable: true,
                    data_type: "INTEGER".to_string(),
                    data_type_id: 1,
                    column_default: None,
                    character_maximum_length: None,
                    character_octet_length: None,
                    numeric_precision: None,
                    numeric_precision_radix: None,
                    numeric_scale: None,
                    datetime_precision: None,
                },
                ColumnData {
                    database_name: "test".to_string(),
                    database_oid: 1,
                    schema_name: "public".to_string(),
                    schema_oid: 10,
                    table_name: "t1".to_string(),
                    table_oid: 100,
                    column_name: "c2".to_string(),
                    column_index: 2,
                    internal: false,
                    is_nullable: true,
                    data_type: "VARCHAR".to_string(),
                    data_type_id: 17,
                    column_default: None,
                    character_maximum_length: None,
                    character_octet_length: None,
                    numeric_precision: None,
                    numeric_precision_radix: None,
                    numeric_scale: None,
                    datetime_precision: None,
                },
            ],
        );

        let state = state_box
            .as_any()
            .downcast_ref::<ParoColumnsGlobalState>()
            .unwrap();

        // 0% progress at start
        assert!((paro_columns_progress(None, Some(state)) - 0.0).abs() < 0.001);

        // Advance offset
        state.offset.store(1, Ordering::Relaxed);
        assert!((paro_columns_progress(None, Some(state)) - 50.0).abs() < 0.001);

        // Complete
        state.offset.store(2, Ordering::Relaxed);
        assert!((paro_columns_progress(None, Some(state)) - 100.0).abs() < 0.001);
    }

    #[test]
    fn test_create_paro_columns_function_set() {
        let set = create_paro_columns_function_set();
        assert_eq!(set.name, "paro_columns");
        assert_eq!(set.functions.len(), 1);

        let func = &set.functions[0];
        assert_eq!(func.name, "paro_columns");
        assert!(func.arguments.is_empty());
        assert!(func.bind.is_some());
        assert!(func.init_global.is_some());
        assert!(func.function.is_some());
        assert!(func.table_scan_progress.is_some());
    }

    #[test]
    fn test_paro_columns_large_batch() {
        let input = TableFunctionInitInput::new(None, &[]);
        let mut state_box = paro_columns_init_global(&input).unwrap().unwrap();

        // Create many columns to test batching
        let state = state_box
            .as_any_mut()
            .downcast_mut::<ParoColumnsGlobalState>()
            .unwrap();

        let mut columns = Vec::new();
        for i in 0..3000 {
            columns.push(ColumnData {
                database_name: "test_db".to_string(),
                database_oid: 1,
                schema_name: "public".to_string(),
                schema_oid: 10,
                table_name: format!("table_{}", i / 10),
                table_oid: 100 + (i / 10) as u64,
                column_name: format!("col_{}", i % 10),
                column_index: (i % 10 + 1) as i32,
                internal: false,
                is_nullable: true,
                data_type: "INTEGER".to_string(),
                data_type_id: 1,
                column_default: None,
                character_maximum_length: None,
                character_octet_length: None,
                numeric_precision: None,
                numeric_precision_radix: None,
                numeric_scale: None,
                datetime_precision: None,
            });
        }
        populate_column_data(state, columns);

        let state_ref = state_box
            .as_any()
            .downcast_ref::<ParoColumnsGlobalState>()
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
                LogicalType::Varchar,
                LogicalType::Integer,
                LogicalType::Boolean,
                LogicalType::Boolean,
                LogicalType::Varchar,
                LogicalType::BigInt,
                LogicalType::Varchar, // column_default
                LogicalType::BigInt,  // character_maximum_length
                LogicalType::BigInt,  // character_octet_length
                LogicalType::BigInt,  // numeric_precision
                LogicalType::BigInt,  // numeric_precision_radix
                LogicalType::BigInt,  // numeric_scale
                LogicalType::BigInt,  // datetime_precision
            ],
            2048,
        );

        // First batch should return 2048 rows
        let result = paro_columns_function(&mut func_input, &mut chunk);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), TableFunctionResult::HaveMoreOutput);
        assert_eq!(chunk.size(), 2048);

        // Second batch should return remaining 952 rows
        let result = paro_columns_function(&mut func_input, &mut chunk);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), TableFunctionResult::Finished);
        assert_eq!(chunk.size(), 952);
    }
}
