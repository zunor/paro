//! paro_storage_info(table) Table Function

use std::any::Any;
use std::sync::atomic::{AtomicUsize, Ordering};

use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use paro_common::vector::Vector;

use crate::table::{
    GlobalTableFunctionState, TableFunction, TableFunctionBindData, TableFunctionBindInput,
    TableFunctionInitInput, TableFunctionInput, TableFunctionResult, TableFunctionSet,
};

#[derive(Clone)]
pub struct ParoStorageInfoBindData {
    pub table_name: String,
}

impl TableFunctionBindData for ParoStorageInfoBindData {
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
pub struct StorageInfoData {
    pub database_name: String,
    pub schema_name: String,
    pub table_name: String,
    pub rowset_id: i64,
    pub segment_id: i64,
    pub column_id: i64,
    pub column_name: String,
    pub column_type: String,
    pub num_rows: i64,
    pub segment_file_size_bytes: i64,
    pub column_size_bytes: i64,
    pub encoding: String,
    pub compression: String,
    pub null_count: Option<i64>,
    pub distinct_count: Option<i64>,
    pub min_value: Option<String>,
    pub max_value: Option<String>,
    pub has_hnsw_index: bool,
    pub has_sparse_index: bool,
    pub has_fulltext_index: bool,
}

pub struct ParoStorageInfoGlobalState {
    pub table_name: String,
    pub entries: Vec<StorageInfoData>,
    pub error: Option<String>,
    pub offset: AtomicUsize,
}

impl GlobalTableFunctionState for ParoStorageInfoGlobalState {
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

fn paro_storage_info_bind(
    input: &TableFunctionBindInput,
    return_types: &mut Vec<LogicalType>,
    names: &mut Vec<String>,
) -> Result<Option<Box<dyn TableFunctionBindData>>> {
    let table_name = match input.inputs.first() {
        Some(Value::Varchar(value)) => value.clone(),
        _ => {
            return Err(paro_error::syntax(
                "paro_storage_info requires a VARCHAR argument (table name)".to_string(),
            ));
        }
    };

    for (name, ty) in [
        ("database_name", LogicalType::Varchar),
        ("schema_name", LogicalType::Varchar),
        ("table_name", LogicalType::Varchar),
        ("rowset_id", LogicalType::BigInt),
        ("segment_id", LogicalType::BigInt),
        ("column_id", LogicalType::BigInt),
        ("column_name", LogicalType::Varchar),
        ("column_type", LogicalType::Varchar),
        ("num_rows", LogicalType::BigInt),
        ("segment_file_size_bytes", LogicalType::BigInt),
        ("column_size_bytes", LogicalType::BigInt),
        ("encoding", LogicalType::Varchar),
        ("compression", LogicalType::Varchar),
        ("null_count", LogicalType::BigInt),
        ("distinct_count", LogicalType::BigInt),
        ("min_value", LogicalType::Varchar),
        ("max_value", LogicalType::Varchar),
        ("has_hnsw_index", LogicalType::Boolean),
        ("has_sparse_index", LogicalType::Boolean),
        ("has_fulltext_index", LogicalType::Boolean),
    ] {
        names.push(name.to_string());
        return_types.push(ty);
    }

    Ok(Some(Box::new(ParoStorageInfoBindData { table_name })))
}

fn paro_storage_info_init_global(
    input: &TableFunctionInitInput,
) -> Result<Option<Box<dyn GlobalTableFunctionState>>> {
    let table_name = input
        .bind_data
        .and_then(|bind_data| bind_data.as_any().downcast_ref::<ParoStorageInfoBindData>())
        .map(|bind_data| bind_data.table_name.clone())
        .unwrap_or_default();

    Ok(Some(Box::new(ParoStorageInfoGlobalState {
        table_name,
        entries: Vec::new(),
        error: None,
        offset: AtomicUsize::new(0),
    })))
}

fn paro_storage_info_function(
    input: &mut TableFunctionInput,
    output: &mut Chunk,
) -> Result<TableFunctionResult> {
    let gstate = input
        .global_state
        .and_then(|gs| gs.as_any().downcast_ref::<ParoStorageInfoGlobalState>());
    let Some(gstate) = gstate else {
        output.set_cardinality(0);
        return Ok(TableFunctionResult::Finished);
    };

    if let Some(message) = &gstate.error {
        return Err(paro_error::table_not_found(message));
    }

    let offset = gstate.offset.load(Ordering::Relaxed);
    if offset >= gstate.entries.len() {
        output.set_cardinality(0);
        return Ok(TableFunctionResult::Finished);
    }

    let batch_size = 2048.min(gstate.entries.len() - offset);
    let mut database_names = Vec::with_capacity(batch_size);
    let mut schema_names = Vec::with_capacity(batch_size);
    let mut table_names = Vec::with_capacity(batch_size);
    let mut rowset_ids = Vec::with_capacity(batch_size);
    let mut segment_ids = Vec::with_capacity(batch_size);
    let mut column_ids = Vec::with_capacity(batch_size);
    let mut column_names = Vec::with_capacity(batch_size);
    let mut column_types = Vec::with_capacity(batch_size);
    let mut num_rows = Vec::with_capacity(batch_size);
    let mut segment_sizes = Vec::with_capacity(batch_size);
    let mut column_sizes = Vec::with_capacity(batch_size);
    let mut encodings = Vec::with_capacity(batch_size);
    let mut compressions = Vec::with_capacity(batch_size);
    let mut null_counts = Vec::with_capacity(batch_size);
    let mut null_count_nulls = Vec::with_capacity(batch_size);
    let mut distinct_counts = Vec::with_capacity(batch_size);
    let mut distinct_count_nulls = Vec::with_capacity(batch_size);
    let mut min_values = Vec::with_capacity(batch_size);
    let mut min_value_nulls = Vec::with_capacity(batch_size);
    let mut max_values = Vec::with_capacity(batch_size);
    let mut max_value_nulls = Vec::with_capacity(batch_size);
    let mut has_hnsw = Vec::with_capacity(batch_size);
    let mut has_sparse = Vec::with_capacity(batch_size);
    let mut has_fulltext = Vec::with_capacity(batch_size);

    for entry in gstate.entries.iter().skip(offset).take(batch_size) {
        database_names.push(entry.database_name.clone());
        schema_names.push(entry.schema_name.clone());
        table_names.push(entry.table_name.clone());
        rowset_ids.push(entry.rowset_id);
        segment_ids.push(entry.segment_id);
        column_ids.push(entry.column_id);
        column_names.push(entry.column_name.clone());
        column_types.push(entry.column_type.clone());
        num_rows.push(entry.num_rows);
        segment_sizes.push(entry.segment_file_size_bytes);
        column_sizes.push(entry.column_size_bytes);
        encodings.push(entry.encoding.clone());
        compressions.push(entry.compression.clone());
        null_counts.push(entry.null_count.unwrap_or_default());
        null_count_nulls.push(entry.null_count.is_none());
        distinct_counts.push(entry.distinct_count.unwrap_or_default());
        distinct_count_nulls.push(entry.distinct_count.is_none());
        min_values.push(entry.min_value.clone().unwrap_or_default());
        min_value_nulls.push(entry.min_value.is_none());
        max_values.push(entry.max_value.clone().unwrap_or_default());
        max_value_nulls.push(entry.max_value.is_none());
        has_hnsw.push(entry.has_hnsw_index);
        has_sparse.push(entry.has_sparse_index);
        has_fulltext.push(entry.has_fulltext_index);
    }

    gstate.offset.fetch_add(batch_size, Ordering::Relaxed);

    for (idx, values) in [
        (
            0usize,
            database_names
                .iter()
                .map(|value| value.as_str())
                .collect::<Vec<_>>(),
        ),
        (
            1usize,
            schema_names
                .iter()
                .map(|value| value.as_str())
                .collect::<Vec<_>>(),
        ),
        (
            2usize,
            table_names
                .iter()
                .map(|value| value.as_str())
                .collect::<Vec<_>>(),
        ),
        (
            6usize,
            column_names
                .iter()
                .map(|value| value.as_str())
                .collect::<Vec<_>>(),
        ),
        (
            7usize,
            column_types
                .iter()
                .map(|value| value.as_str())
                .collect::<Vec<_>>(),
        ),
        (
            11usize,
            encodings
                .iter()
                .map(|value| value.as_str())
                .collect::<Vec<_>>(),
        ),
        (
            12usize,
            compressions
                .iter()
                .map(|value| value.as_str())
                .collect::<Vec<_>>(),
        ),
        (
            15usize,
            min_values
                .iter()
                .map(|value| value.as_str())
                .collect::<Vec<_>>(),
        ),
        (
            16usize,
            max_values
                .iter()
                .map(|value| value.as_str())
                .collect::<Vec<_>>(),
        ),
    ] {
        if let Some(col) = output.column_mut(idx) {
            *col = Vector::from_strings(&values);
        }
    }

    for (idx, values) in [
        (3usize, &rowset_ids),
        (4usize, &segment_ids),
        (5usize, &column_ids),
        (8usize, &num_rows),
        (9usize, &segment_sizes),
        (10usize, &column_sizes),
        (13usize, &null_counts),
        (14usize, &distinct_counts),
    ] {
        if let Some(col) = output.column_mut(idx) {
            *col = Vector::from_i64(values);
        }
    }

    for (idx, values) in [
        (17usize, &has_hnsw),
        (18usize, &has_sparse),
        (19usize, &has_fulltext),
    ] {
        if let Some(col) = output.column_mut(idx) {
            *col = Vector::from_bool(values);
        }
    }

    if let Some(col) = output.column_mut(13) {
        for (idx, is_null) in null_count_nulls.iter().enumerate() {
            if *is_null {
                col.set_null(idx, true);
            }
        }
    }
    if let Some(col) = output.column_mut(14) {
        for (idx, is_null) in distinct_count_nulls.iter().enumerate() {
            if *is_null {
                col.set_null(idx, true);
            }
        }
    }
    if let Some(col) = output.column_mut(15) {
        for (idx, is_null) in min_value_nulls.iter().enumerate() {
            if *is_null {
                col.set_null(idx, true);
            }
        }
    }
    if let Some(col) = output.column_mut(16) {
        for (idx, is_null) in max_value_nulls.iter().enumerate() {
            if *is_null {
                col.set_null(idx, true);
            }
        }
    }

    output.set_cardinality(batch_size);
    if gstate.offset.load(Ordering::Relaxed) >= gstate.entries.len() {
        Ok(TableFunctionResult::Finished)
    } else {
        Ok(TableFunctionResult::HaveMoreOutput)
    }
}

fn paro_storage_info_progress(
    _bind_data: Option<&dyn TableFunctionBindData>,
    global_state: Option<&dyn GlobalTableFunctionState>,
) -> f64 {
    global_state
        .map(|state| state.get_progress())
        .unwrap_or(-1.0)
}

pub fn create_paro_storage_info_function_set() -> TableFunctionSet {
    let mut func = TableFunction::new("paro_storage_info", vec![LogicalType::Varchar]);
    func.bind = Some(paro_storage_info_bind);
    func.init_global = Some(paro_storage_info_init_global);
    func.function = Some(paro_storage_info_function);
    func.table_scan_progress = Some(paro_storage_info_progress);

    let mut set = TableFunctionSet::new("paro_storage_info");
    set.add_function(func);
    set
}

pub fn populate_storage_info_data(
    state: &mut ParoStorageInfoGlobalState,
    entries: Vec<StorageInfoData>,
    error: Option<String>,
) {
    state.entries = entries;
    state.error = error;
    state.offset.store(0, Ordering::Relaxed);
}
