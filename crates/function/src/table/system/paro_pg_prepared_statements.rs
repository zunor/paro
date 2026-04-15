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
pub struct ParoPgPreparedStatementsBindData;

impl TableFunctionBindData for ParoPgPreparedStatementsBindData {
    fn clone_box(&self) -> Box<dyn TableFunctionBindData> {
        Box::new(self.clone())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[derive(Debug, Clone, Default)]
pub struct PreparedStatementSummaryData {
    pub name: String,
    pub statement: String,
    pub parameter_types: String,
    pub from_sql: bool,
    pub generic_plans: i64,
    pub custom_plans: i64,
}

pub struct ParoPgPreparedStatementsGlobalState {
    pub rows: Vec<PreparedStatementSummaryData>,
    pub offset: AtomicUsize,
}

impl GlobalTableFunctionState for ParoPgPreparedStatementsGlobalState {
    fn max_threads(&self) -> usize {
        1
    }

    fn get_progress(&self) -> f64 {
        if self.rows.is_empty() {
            100.0
        } else {
            (self.offset.load(Ordering::Relaxed) as f64 / self.rows.len() as f64) * 100.0
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

fn bind(
    _input: &TableFunctionBindInput,
    return_types: &mut Vec<LogicalType>,
    names: &mut Vec<String>,
) -> Result<Option<Box<dyn TableFunctionBindData>>> {
    let columns = [
        ("name", LogicalType::Varchar),
        ("statement", LogicalType::Varchar),
        ("parameter_types", LogicalType::Varchar),
        ("from_sql", LogicalType::Boolean),
        ("generic_plans", LogicalType::BigInt),
        ("custom_plans", LogicalType::BigInt),
    ];

    for (name, ty) in columns {
        names.push(name.to_string());
        return_types.push(ty);
    }

    Ok(Some(Box::new(ParoPgPreparedStatementsBindData)))
}

fn init_global(
    _input: &TableFunctionInitInput,
) -> Result<Option<Box<dyn GlobalTableFunctionState>>> {
    Ok(Some(Box::new(ParoPgPreparedStatementsGlobalState {
        rows: Vec::new(),
        offset: AtomicUsize::new(0),
    })))
}

fn function(input: &mut TableFunctionInput, output: &mut Chunk) -> Result<TableFunctionResult> {
    let Some(state) = input.global_state.and_then(|gs| {
        gs.as_any()
            .downcast_ref::<ParoPgPreparedStatementsGlobalState>()
    }) else {
        output.set_cardinality(0);
        return Ok(TableFunctionResult::Finished);
    };

    let offset = state.offset.load(Ordering::Relaxed);
    if offset >= state.rows.len() {
        output.set_cardinality(0);
        return Ok(TableFunctionResult::Finished);
    }

    let batch_size = 2048.min(state.rows.len() - offset);
    let slice = &state.rows[offset..offset + batch_size];

    let names: Vec<&str> = slice.iter().map(|row| row.name.as_str()).collect();
    let statements: Vec<&str> = slice.iter().map(|row| row.statement.as_str()).collect();
    let parameter_types: Vec<&str> = slice
        .iter()
        .map(|row| row.parameter_types.as_str())
        .collect();
    let from_sql: Vec<bool> = slice.iter().map(|row| row.from_sql).collect();
    let generic_plans: Vec<i64> = slice.iter().map(|row| row.generic_plans).collect();
    let custom_plans: Vec<i64> = slice.iter().map(|row| row.custom_plans).collect();

    if let Some(col) = output.column_mut(0) {
        *col = Vector::from_strings(&names);
    }
    if let Some(col) = output.column_mut(1) {
        *col = Vector::from_strings(&statements);
    }
    if let Some(col) = output.column_mut(2) {
        *col = Vector::from_strings(&parameter_types);
    }
    if let Some(col) = output.column_mut(3) {
        *col = Vector::from_bool(&from_sql);
    }
    if let Some(col) = output.column_mut(4) {
        *col = Vector::from_i64(&generic_plans);
    }
    if let Some(col) = output.column_mut(5) {
        *col = Vector::from_i64(&custom_plans);
    }

    output.set_cardinality(batch_size);
    state.offset.fetch_add(batch_size, Ordering::Relaxed);

    if state.offset.load(Ordering::Relaxed) >= state.rows.len() {
        Ok(TableFunctionResult::Finished)
    } else {
        Ok(TableFunctionResult::HaveMoreOutput)
    }
}

fn progress(
    _bind_data: Option<&dyn TableFunctionBindData>,
    global_state: Option<&dyn GlobalTableFunctionState>,
) -> f64 {
    global_state.map(|s| s.get_progress()).unwrap_or(-1.0)
}

pub fn create_paro_pg_prepared_statements_function_set() -> TableFunctionSet {
    let mut func = TableFunction::new("paro_pg_prepared_statements", vec![]);
    func.bind = Some(bind);
    func.init_global = Some(init_global);
    func.function = Some(function);
    func.table_scan_progress = Some(progress);

    let mut set = TableFunctionSet::new("paro_pg_prepared_statements");
    set.add_function(func);
    set
}

pub fn populate_prepared_statement_data(
    state: &mut ParoPgPreparedStatementsGlobalState,
    rows: Vec<PreparedStatementSummaryData>,
) {
    state.rows = rows;
    state.offset.store(0, Ordering::Relaxed);
}
