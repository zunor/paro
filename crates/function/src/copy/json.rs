use std::fs::File;
use std::io::{BufWriter, Write};

use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use serde_json::{Map as JsonMap, Number as JsonNumber, Value as JsonValue};

use super::{
    CopyFormat, CopyFunction, CopyFunctionBindData, CopyOptions, CopyToGlobalState,
    CopyToLocalState,
};
use crate::table::read_ndjson::create_read_ndjson_function;
use crate::table::TableFunction;

#[derive(Debug)]
struct NdjsonCopyBindData {
    names: Vec<String>,
}

impl CopyFunctionBindData for NdjsonCopyBindData {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

struct NdjsonCopyToGlobalState {
    writer: BufWriter<File>,
}

impl CopyToGlobalState for NdjsonCopyToGlobalState {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

#[derive(Default)]
struct NdjsonCopyToLocalState;

impl CopyToLocalState for NdjsonCopyToLocalState {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

pub fn register_copy_functions() -> Vec<CopyFunction> {
    vec![register_ndjson_copy_function()]
}

pub fn register_ndjson_copy_function() -> CopyFunction {
    CopyFunction {
        name: "ndjson".to_string(),
        copy_to_bind: ndjson_copy_to_bind,
        copy_to_initialize_global: ndjson_copy_to_initialize_global,
        copy_to_initialize_local: ndjson_copy_to_initialize_local,
        copy_to_sink: ndjson_copy_to_sink,
        copy_to_combine: ndjson_copy_to_combine,
        copy_to_finalize: ndjson_copy_to_finalize,
        copy_from_bind: ndjson_copy_from_bind,
        copy_from_function: read_ndjson_table_function(),
        extension: "json".to_string(),
    }
}

fn read_ndjson_table_function() -> TableFunction {
    create_read_ndjson_function()
}

fn ndjson_copy_to_bind(
    options: &CopyOptions,
    names: &[String],
    types: &[LogicalType],
) -> Result<Box<dyn CopyFunctionBindData>> {
    if names.len() != types.len() {
        return Err(paro_error::invalid_input(
            "COPY TO input names/types length mismatch",
        ));
    }
    if !matches!(options.format, CopyFormat::Ndjson) {
        return Err(paro_error::invalid_parameter(
            "NDJSON copy function requires FORMAT ndjson",
        ));
    }

    Ok(Box::new(NdjsonCopyBindData {
        names: names.to_vec(),
    }))
}

fn ndjson_copy_from_bind(
    options: &CopyOptions,
    names: &[String],
    types: &[LogicalType],
) -> Result<Box<dyn CopyFunctionBindData>> {
    if names.len() != types.len() {
        return Err(paro_error::invalid_input(
            "COPY FROM input names/types length mismatch",
        ));
    }
    if !matches!(options.format, CopyFormat::Ndjson) {
        return Err(paro_error::invalid_parameter(
            "NDJSON copy function requires FORMAT ndjson",
        ));
    }

    Ok(Box::new(NdjsonCopyBindData {
        names: names.to_vec(),
    }))
}

fn ndjson_copy_to_initialize_global(
    _bind_data: &dyn CopyFunctionBindData,
    file_path: &str,
) -> Result<Box<dyn CopyToGlobalState>> {
    let file = File::create(file_path).map_err(|e| {
        paro_error::io_error(format!(
            "Failed to create COPY output file '{}': {}",
            file_path, e
        ))
    })?;
    Ok(Box::new(NdjsonCopyToGlobalState {
        writer: BufWriter::new(file),
    }))
}

fn ndjson_copy_to_initialize_local(
    _bind_data: &dyn CopyFunctionBindData,
) -> Result<Box<dyn CopyToLocalState>> {
    Ok(Box::new(NdjsonCopyToLocalState))
}

fn ndjson_copy_to_sink(
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
        .downcast_ref::<NdjsonCopyBindData>()
        .ok_or_else(|| paro_error::internal("Invalid NDJSON bind data".to_string()))?;
    let global_state = global_state
        .as_any_mut()
        .downcast_mut::<NdjsonCopyToGlobalState>()
        .ok_or_else(|| paro_error::internal("Invalid NDJSON global state".to_string()))?;

    for row in 0..chunk.len() {
        let mut object = JsonMap::with_capacity(chunk.column_count());
        for col in 0..chunk.column_count() {
            let key = bind_data
                .names
                .get(col)
                .cloned()
                .unwrap_or_else(|| format!("column{}", col + 1));
            let value = chunk.data[col].get_value(row);
            object.insert(key, value_to_json(&value));
        }
        serde_json::to_writer(&mut global_state.writer, &JsonValue::Object(object))
            .map_err(|e| paro_error::io_error(format!("Failed to write NDJSON row: {}", e)))?;
        global_state
            .writer
            .write_all(b"\n")
            .map_err(|e| paro_error::io_error(format!("Failed to write NDJSON row: {}", e)))?;
    }

    Ok(())
}

fn ndjson_copy_to_combine(
    _bind_data: &dyn CopyFunctionBindData,
    _global_state: &mut dyn CopyToGlobalState,
    _local_state: &mut dyn CopyToLocalState,
) -> Result<()> {
    Ok(())
}

fn ndjson_copy_to_finalize(
    _bind_data: &dyn CopyFunctionBindData,
    global_state: &mut dyn CopyToGlobalState,
) -> Result<()> {
    let global_state = global_state
        .as_any_mut()
        .downcast_mut::<NdjsonCopyToGlobalState>()
        .ok_or_else(|| paro_error::internal("Invalid NDJSON global state".to_string()))?;
    global_state
        .writer
        .flush()
        .map_err(|e| paro_error::io_error(format!("Failed to flush NDJSON output: {}", e)))?;
    Ok(())
}

fn value_to_json(value: &Value) -> JsonValue {
    match value {
        Value::Null(_) => JsonValue::Null,
        Value::Boolean(v) => JsonValue::Bool(*v),
        Value::TinyInt(v) => JsonValue::Number(JsonNumber::from(*v)),
        Value::SmallInt(v) => JsonValue::Number(JsonNumber::from(*v)),
        Value::Integer(v) => JsonValue::Number(JsonNumber::from(*v)),
        Value::BigInt(v) => JsonValue::Number(JsonNumber::from(*v)),
        Value::UTinyInt(v) => JsonValue::Number(JsonNumber::from(*v)),
        Value::USmallInt(v) => JsonValue::Number(JsonNumber::from(*v)),
        Value::UInteger(v) => JsonValue::Number(JsonNumber::from(*v)),
        Value::UBigInt(v) => JsonValue::Number(JsonNumber::from(*v)),
        Value::Float(v) => JsonNumber::from_f64(*v as f64)
            .map(JsonValue::Number)
            .unwrap_or(JsonValue::Null),
        Value::Double(v) => JsonNumber::from_f64(*v)
            .map(JsonValue::Number)
            .unwrap_or(JsonValue::Null),
        Value::Varchar(v) => JsonValue::String(v.clone()),
        Value::List(values, _) | Value::Array(values, _, _) => {
            JsonValue::Array(values.iter().map(value_to_json).collect())
        }
        Value::Struct(values, fields) => {
            let mut object = JsonMap::with_capacity(fields.len());
            for (idx, (name, _)) in fields.iter().enumerate() {
                object.insert(
                    name.clone(),
                    values
                        .get(idx)
                        .map(value_to_json)
                        .unwrap_or(JsonValue::Null),
                );
            }
            JsonValue::Object(object)
        }
        _ => JsonValue::String(value.to_string()),
    }
}
