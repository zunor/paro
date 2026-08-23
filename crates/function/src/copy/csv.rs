// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::fs::File;
use std::io::{BufWriter, Write};

use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;

use super::{
    CopyFormat, CopyFromFunction, CopyFromSource, CopyFunction, CopyFunctionBindData, CopyOptions,
    CopyToFunction, CopyToGlobalState, CopyToLocalState, ForceQuoteOption,
};
use crate::table::read_binary::{
    bind_copy_from as bind_binary_copy_from, create_read_binary_function,
};
use crate::table::read_csv::{bind_copy_from, create_read_csv_function};
use crate::table::{TableFunction, TableFunctionBindData};

#[derive(Debug)]
struct CsvCopyToBindData {
    names: Vec<String>,
    delimiter: String,
    null_string: String,
    header: bool,
    quote: Option<char>,
    escape: Option<char>,
    force_quote_columns: Vec<bool>,
}

impl CopyFunctionBindData for CsvCopyToBindData {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

struct CsvCopyToGlobalState {
    writer: BufWriter<File>,
    written_rows: u64,
}

impl CopyToGlobalState for CsvCopyToGlobalState {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

#[derive(Default)]
struct CsvCopyToLocalState;

impl CopyToLocalState for CsvCopyToLocalState {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

pub fn register_copy_functions() -> Vec<CopyFunction> {
    vec![
        register_csv_copy_function(),
        register_text_copy_function(),
        register_binary_copy_function(),
    ]
}

pub fn register_csv_copy_function() -> CopyFunction {
    CopyFunction {
        name: "csv".to_string(),
        copy_to: Some(CopyToFunction {
            copy_to_bind: csv_copy_to_bind,
            copy_to_initialize_global: csv_copy_to_initialize_global,
            copy_to_initialize_local: csv_copy_to_initialize_local,
            copy_to_sink: csv_copy_to_sink,
            copy_to_combine: csv_copy_to_combine,
            copy_to_finalize: csv_copy_to_finalize,
        }),
        copy_from: Some(CopyFromFunction {
            copy_from_bind: csv_copy_from_bind,
            copy_from_function: read_csv_table_function(),
        }),
        extension: "csv".to_string(),
    }
}

pub fn register_text_copy_function() -> CopyFunction {
    let mut func = register_csv_copy_function();
    func.name = "text".to_string();
    func.extension = "txt".to_string();
    func
}

pub fn register_binary_copy_function() -> CopyFunction {
    CopyFunction {
        name: "binary".to_string(),
        copy_to: None,
        copy_from: Some(CopyFromFunction {
            copy_from_bind: binary_copy_from_bind,
            copy_from_function: create_read_binary_function(),
        }),
        extension: "bin".to_string(),
    }
}

fn binary_copy_from_bind(
    source: CopyFromSource,
    options: &CopyOptions,
    names: &[String],
    types: &[LogicalType],
) -> Result<Box<dyn TableFunctionBindData>> {
    if !matches!(options.format, CopyFormat::Binary) {
        return Err(paro_error::invalid_parameter(
            "binary copy function requires FORMAT binary",
        ));
    }
    bind_binary_copy_from(source, names, types)
}

fn read_csv_table_function() -> TableFunction {
    create_read_csv_function()
}

fn csv_copy_from_bind(
    source: CopyFromSource,
    options: &CopyOptions,
    names: &[String],
    types: &[LogicalType],
) -> Result<Box<dyn TableFunctionBindData>> {
    if matches!(options.format, CopyFormat::Ndjson) {
        return Err(paro_error::invalid_parameter(
            "CSV/TEXT copy function does not support FORMAT ndjson",
        ));
    }

    bind_copy_from(source, options, names, types)
}

fn csv_copy_to_bind(
    options: &CopyOptions,
    names: &[String],
    types: &[LogicalType],
) -> Result<Box<dyn CopyFunctionBindData>> {
    if names.len() != types.len() {
        return Err(paro_error::invalid_input(
            "COPY TO input names/types length mismatch",
        ));
    }

    if matches!(options.format, CopyFormat::Ndjson) {
        return Err(paro_error::invalid_parameter(
            "CSV/TEXT copy function does not support FORMAT ndjson",
        ));
    }

    let delimiter = options
        .delimiter()
        .expect("CSV/TEXT options always have a delimiter")
        .to_string();
    if delimiter.is_empty() {
        return Err(paro_error::invalid_parameter(
            "COPY option delimiter cannot be empty",
        ));
    }

    let null_string = options
        .null_string()
        .expect("CSV/TEXT options always have a NULL marker")
        .to_string();

    let header = options.header();

    let quote = options.quote();
    let escape = options.escape();

    if matches!(options.format, CopyFormat::Csv) && quote.is_none() {
        return Err(paro_error::invalid_parameter(
            "COPY option quote is required for CSV format",
        ));
    }

    if matches!(options.format, CopyFormat::Csv) && escape.is_none() {
        return Err(paro_error::invalid_parameter(
            "COPY option escape is required for CSV format",
        ));
    }

    let force_quote_columns =
        resolve_force_quote_columns(&options.force_quote, names, options.format)?;

    Ok(Box::new(CsvCopyToBindData {
        names: names.to_vec(),
        delimiter,
        null_string,
        header,
        quote,
        escape,
        force_quote_columns,
    }))
}

fn csv_copy_to_initialize_global(
    bind_data: &dyn CopyFunctionBindData,
    file_path: &str,
) -> Result<Box<dyn CopyToGlobalState>> {
    let bind_data = bind_data
        .as_any()
        .downcast_ref::<CsvCopyToBindData>()
        .ok_or_else(|| paro_error::internal("Invalid CSV bind data".to_string()))?;

    let file = File::create(file_path).map_err(|e| {
        paro_error::io_error(format!(
            "Failed to create COPY output file '{}': {}",
            file_path, e
        ))
    })?;
    let mut writer = BufWriter::new(file);

    if bind_data.header {
        write_header(&mut writer, bind_data)?;
    }

    Ok(Box::new(CsvCopyToGlobalState {
        writer,
        written_rows: 0,
    }))
}

fn csv_copy_to_initialize_local(
    _bind_data: &dyn CopyFunctionBindData,
) -> Result<Box<dyn CopyToLocalState>> {
    Ok(Box::new(CsvCopyToLocalState::default()))
}

fn csv_copy_to_sink(
    bind_data: &dyn CopyFunctionBindData,
    global_state: &mut dyn CopyToGlobalState,
    _local_state: &mut dyn CopyToLocalState,
    chunk: &Chunk,
) -> Result<()> {
    if chunk.is_empty() {
        return Ok(());
    }

    let bind_data = bind_data
        .as_any()
        .downcast_ref::<CsvCopyToBindData>()
        .ok_or_else(|| paro_error::internal("Invalid CSV bind data".to_string()))?;

    let global_state = global_state
        .as_any_mut()
        .downcast_mut::<CsvCopyToGlobalState>()
        .ok_or_else(|| paro_error::internal("Invalid CSV global state".to_string()))?;

    let mut line = String::new();
    let delimiter = &bind_data.delimiter;

    for row in 0..chunk.len() {
        line.clear();
        for col in 0..chunk.column_count() {
            if col > 0 {
                line.push_str(delimiter);
            }

            let value = chunk.data[col].get_value(row);
            let field = value_to_string(&value, &bind_data.null_string);
            let force_quote = bind_data
                .force_quote_columns
                .get(col)
                .copied()
                .unwrap_or(false);
            append_field(&mut line, &field, bind_data, force_quote);
        }
        line.push('\n');
        global_state
            .writer
            .write_all(line.as_bytes())
            .map_err(|e| paro_error::io_error(format!("Failed to write COPY output: {}", e)))?;
    }

    global_state.written_rows += chunk.len() as u64;
    Ok(())
}

fn csv_copy_to_combine(
    _bind_data: &dyn CopyFunctionBindData,
    _global_state: &mut dyn CopyToGlobalState,
    _local_state: &mut dyn CopyToLocalState,
) -> Result<()> {
    Ok(())
}

fn csv_copy_to_finalize(
    _bind_data: &dyn CopyFunctionBindData,
    global_state: &mut dyn CopyToGlobalState,
) -> Result<()> {
    let global_state = global_state
        .as_any_mut()
        .downcast_mut::<CsvCopyToGlobalState>()
        .ok_or_else(|| paro_error::internal("Invalid CSV global state".to_string()))?;
    global_state
        .writer
        .flush()
        .map_err(|e| paro_error::io_error(format!("Failed to flush COPY output: {}", e)))?;
    Ok(())
}

fn resolve_force_quote_columns(
    option: &ForceQuoteOption,
    names: &[String],
    format: CopyFormat,
) -> Result<Vec<bool>> {
    if matches!(format, CopyFormat::Text) && !matches!(option, ForceQuoteOption::None) {
        // TEXT mode does not support FORCE_QUOTE; reject directly to avoid implicit behavior.
        return Err(paro_error::invalid_parameter(
            "FORCE_QUOTE is only supported for CSV format",
        ));
    }

    let mut result = vec![false; names.len()];
    match option {
        ForceQuoteOption::None => {}
        ForceQuoteOption::All => {
            for item in result.iter_mut() {
                *item = true;
            }
        }
        ForceQuoteOption::Columns(columns) => {
            for col in columns {
                if let Some(idx) = names.iter().position(|name| name.eq_ignore_ascii_case(col)) {
                    result[idx] = true;
                } else {
                    return Err(paro_error::invalid_parameter(format!(
                        "FORCE_QUOTE column '{}' not found in COPY output",
                        col
                    )));
                }
            }
        }
    }

    Ok(result)
}

fn write_header(writer: &mut BufWriter<File>, bind_data: &CsvCopyToBindData) -> Result<()> {
    let mut line = String::new();
    for (idx, name) in bind_data.names.iter().enumerate() {
        if idx > 0 {
            line.push_str(&bind_data.delimiter);
        }
        append_field(&mut line, name, bind_data, false);
    }
    line.push('\n');
    writer
        .write_all(line.as_bytes())
        .map_err(|e| paro_error::io_error(format!("Failed to write COPY header: {}", e)))?;
    Ok(())
}

fn append_field(out: &mut String, field: &str, bind_data: &CsvCopyToBindData, force_quote: bool) {
    let quote = bind_data.quote;
    let escape = bind_data.escape.or(quote);
    let needs_quote = should_quote(field, bind_data, force_quote);

    if needs_quote {
        let quote_char = quote.unwrap_or('"');
        let escape_char = escape.unwrap_or(quote_char);
        out.push(quote_char);
        for ch in field.chars() {
            if ch == quote_char || ch == escape_char {
                out.push(escape_char);
            }
            out.push(ch);
        }
        out.push(quote_char);
    } else {
        out.push_str(field);
    }
}

fn should_quote(field: &str, bind_data: &CsvCopyToBindData, force_quote: bool) -> bool {
    if force_quote {
        return true;
    }

    let quote = match bind_data.quote {
        Some(q) => q,
        None => return false,
    };

    if field.contains('\n') || field.contains('\r') {
        return true;
    }

    if !bind_data.delimiter.is_empty() && field.contains(&bind_data.delimiter) {
        return true;
    }

    field.contains(quote)
}

fn value_to_string(value: &Value, null_string: &str) -> String {
    match value {
        Value::Null(_) => null_string.to_string(),
        Value::Varchar(v) => v.clone(),
        _ => value.to_string(),
    }
}
