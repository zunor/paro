//! read_ndjson Table Function
//!
//! Provides NDJSON reader for COPY FROM... WITH (FORMAT ndjson/json).

use std::any::Any;
use std::io::BufRead;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;

use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use paro_common::vector::VECTOR_SIZE;
use serde_json::Value as JsonValue;

use crate::copy::CopyFormat;

use super::read_csv::open_csv_reader;
use super::{
    GlobalTableFunctionState, LocalTableFunctionState, TableFunction, TableFunctionBindData,
    TableFunctionBindInput, TableFunctionInitInput, TableFunctionInput, TableFunctionResult,
    TableFunctionSet,
};

#[derive(Clone, Debug, Default)]
struct ReadNdjsonOptions;

impl ReadNdjsonOptions {
    fn from_value(value: Option<&Value>) -> Result<Self> {
        if let Some(value) = value {
            let Value::Struct(values, fields) = value else {
                return Err(paro_error::invalid_parameter(
                    "read_ndjson options must be a STRUCT",
                ));
            };
            if values.len() != fields.len() {
                return Err(paro_error::internal(
                    "read_ndjson options struct length mismatch",
                ));
            }

            for (value, (name, _ty)) in values.iter().zip(fields.iter()) {
                match name.to_lowercase().as_str() {
                    "format" => {
                        if let Some(v) = value_as_string(value)? {
                            let format = CopyFormat::parse(&v)?;
                            if !matches!(format, CopyFormat::Ndjson) {
                                return Err(paro_error::invalid_parameter(
                                    "read_ndjson requires FORMAT ndjson/json",
                                ));
                            }
                        }
                    }
                    // keep compatibility with COPY option struct shape
                    "delimiter" | "null" | "null_string" | "header" | "quote" | "escape"
                    | "parallel" | "parallel_workers" => {}
                    _ => {
                        return Err(paro_error::invalid_parameter(format!(
                            "unknown read_ndjson option: {}",
                            name
                        )));
                    }
                }
            }
        }
        Ok(Self)
    }
}

#[derive(Clone, Debug)]
struct ReadNdjsonBindData {
    file_path: String,
    options: ReadNdjsonOptions,
    names: Vec<String>,
    types: Vec<LogicalType>,
}

impl TableFunctionBindData for ReadNdjsonBindData {
    fn clone_box(&self) -> Box<dyn TableFunctionBindData> {
        Box::new(self.clone())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

struct ReadNdjsonGlobalState {
    reader: Mutex<ReadNdjsonReader>,
    finished: AtomicBool,
}

struct ReadNdjsonReader {
    reader: Box<dyn BufRead + Send>,
    row_number: usize,
}

impl GlobalTableFunctionState for ReadNdjsonGlobalState {
    fn max_threads(&self) -> usize {
        1
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

#[derive(Default)]
struct ReadNdjsonLocalState {
    line_buffer: String,
}

impl LocalTableFunctionState for ReadNdjsonLocalState {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

pub fn create_read_ndjson_function() -> TableFunction {
    TableFunction::new("read_ndjson", vec![LogicalType::Varchar])
        .with_bind(read_ndjson_bind)
        .with_init_global(read_ndjson_init_global)
        .with_init_local(read_ndjson_init_local)
        .with_function(read_ndjson_function)
}

pub fn create_read_ndjson_function_set() -> TableFunctionSet {
    let mut set = TableFunctionSet::new("read_ndjson");
    set.add_function(create_read_ndjson_function());
    set
}

fn read_ndjson_bind(
    input: &TableFunctionBindInput,
    return_types: &mut Vec<LogicalType>,
    return_names: &mut Vec<String>,
) -> Result<Option<Box<dyn TableFunctionBindData>>> {
    let path_value = input
        .inputs
        .get(0)
        .ok_or_else(|| paro_error::invalid_parameter("read_ndjson expects a file path argument"))?;
    let file_path = match path_value {
        Value::Varchar(path) => path.clone(),
        _ => {
            return Err(paro_error::invalid_parameter(
                "read_ndjson path must be VARCHAR",
            ))
        }
    };

    let mut schema: Option<(Vec<String>, Vec<LogicalType>)> = None;
    let mut options_value: Option<&Value> = None;

    if let Some(value) = input.inputs.get(2) {
        let schema_value = input
            .inputs
            .get(1)
            .ok_or_else(|| paro_error::invalid_parameter("read_ndjson schema argument missing"))?;
        schema = Some(extract_schema(schema_value)?);
        options_value = Some(value);
    } else if let Some(value) = input.inputs.get(1) {
        if let Value::Struct(_, fields) = value {
            if is_option_struct(fields) {
                options_value = Some(value);
            } else {
                schema = Some(extract_schema(value)?);
            }
        } else {
            options_value = Some(value);
        }
    }

    let options = ReadNdjsonOptions::from_value(options_value)?;
    let (names, types) = schema.ok_or_else(|| {
        paro_error::invalid_parameter("read_ndjson requires an explicit output schema")
    })?;

    return_types.extend(types.iter().cloned());
    return_names.extend(names.iter().cloned());

    Ok(Some(Box::new(ReadNdjsonBindData {
        file_path,
        options,
        names,
        types,
    })))
}

fn read_ndjson_init_global(
    input: &TableFunctionInitInput,
) -> Result<Option<Box<dyn GlobalTableFunctionState>>> {
    let bind_data = input
        .bind_data
        .and_then(|data| data.as_any().downcast_ref::<ReadNdjsonBindData>())
        .ok_or_else(|| paro_error::internal("Invalid read_ndjson bind data".to_string()))?;

    let reader = open_csv_reader(&bind_data.file_path)?;
    Ok(Some(Box::new(ReadNdjsonGlobalState {
        reader: Mutex::new(ReadNdjsonReader {
            reader,
            row_number: 0,
        }),
        finished: AtomicBool::new(false),
    })))
}

fn read_ndjson_init_local(
    _input: &TableFunctionInitInput,
    _global_state: Option<&dyn GlobalTableFunctionState>,
) -> Result<Option<Box<dyn LocalTableFunctionState>>> {
    Ok(Some(Box::new(ReadNdjsonLocalState::default())))
}

fn read_ndjson_function(
    input: &mut TableFunctionInput,
    output: &mut Chunk,
) -> Result<TableFunctionResult> {
    let bind_data = input
        .bind_data
        .and_then(|data| data.as_any().downcast_ref::<ReadNdjsonBindData>())
        .ok_or_else(|| paro_error::internal("Invalid read_ndjson bind data".to_string()))?;
    let global_state = input
        .global_state
        .and_then(|state| state.as_any().downcast_ref::<ReadNdjsonGlobalState>())
        .ok_or_else(|| paro_error::internal("Invalid read_ndjson global state".to_string()))?;
    let local_state = input
        .local_state
        .as_mut()
        .and_then(|state| state.as_any_mut().downcast_mut::<ReadNdjsonLocalState>())
        .ok_or_else(|| paro_error::internal("Invalid read_ndjson local state".to_string()))?;

    if global_state.finished.load(Ordering::SeqCst) {
        output.set_cardinality(0);
        return Ok(TableFunctionResult::Finished);
    }

    let mut guard = global_state
        .reader
        .lock()
        .map_err(|e| paro_error::internal(e.to_string()))?;

    let mut produced = 0;
    let capacity = output.capacity().min(VECTOR_SIZE);
    while produced < capacity {
        let record =
            read_ndjson_record(&mut guard, &bind_data.options, &mut local_state.line_buffer)?;
        let Some(json_obj) = record else {
            global_state.finished.store(true, Ordering::SeqCst);
            break;
        };

        for col_idx in 0..bind_data.types.len() {
            let col_name = bind_data
                .names
                .get(col_idx)
                .ok_or_else(|| paro_error::internal("column name missing".to_string()))?;
            let json_value = lookup_json_value(&json_obj, col_name);
            let value = parse_json_field(json_value, &bind_data.types[col_idx]).map_err(|e| {
                paro_error::invalid_value(
                    format!("read_ndjson row {}", guard.row_number),
                    e.to_string(),
                )
            })?;
            let col = output
                .column_mut(col_idx)
                .ok_or_else(|| paro_error::internal("Output column not found".to_string()))?;
            col.set_value(produced, &value);
        }

        produced += 1;
    }

    output.set_cardinality(produced);
    if global_state.finished.load(Ordering::SeqCst) {
        Ok(TableFunctionResult::Finished)
    } else {
        Ok(TableFunctionResult::HaveMoreOutput)
    }
}

fn read_ndjson_record(
    reader: &mut ReadNdjsonReader,
    _options: &ReadNdjsonOptions,
    buffer: &mut String,
) -> Result<Option<serde_json::Map<String, JsonValue>>> {
    loop {
        buffer.clear();
        let bytes = reader
            .reader
            .read_line(buffer)
            .map_err(|e| paro_error::io_error(format!("Failed to read NDJSON: {}", e)))?;
        if bytes == 0 {
            return Ok(None);
        }
        reader.row_number += 1;

        while buffer.ends_with('\n') || buffer.ends_with('\r') {
            buffer.pop();
        }
        if buffer.trim().is_empty() {
            continue;
        }

        let json_value: JsonValue = serde_json::from_str(buffer).map_err(|e| {
            paro_error::invalid_parameter(format!(
                "Invalid NDJSON record at row {}: {}",
                reader.row_number, e
            ))
        })?;
        let JsonValue::Object(obj) = json_value else {
            return Err(paro_error::invalid_parameter(format!(
                "Invalid NDJSON record at row {}: expected a JSON object",
                reader.row_number
            )));
        };
        return Ok(Some(obj));
    }
}

fn lookup_json_value<'a>(
    obj: &'a serde_json::Map<String, JsonValue>,
    key: &str,
) -> Option<&'a JsonValue> {
    if let Some(value) = obj.get(key) {
        return Some(value);
    }
    obj.iter().find_map(|(name, value)| {
        if name.eq_ignore_ascii_case(key) {
            return Some(value);
        }
        name.rsplit_once('.')
            .and_then(|(_, suffix)| suffix.eq_ignore_ascii_case(key).then_some(value))
    })
}

fn parse_json_field(value: Option<&JsonValue>, target_type: &LogicalType) -> Result<Value> {
    let Some(value) = value else {
        return Ok(Value::Null(target_type.clone()));
    };
    if value.is_null() {
        return Ok(Value::Null(target_type.clone()));
    }

    match target_type {
        LogicalType::Varchar
        | LogicalType::VarcharCollation(_)
        | LogicalType::Json
        | LogicalType::Jsonb
        | LogicalType::TsVector
        | LogicalType::TsQuery => match value {
            JsonValue::String(v) => Ok(Value::Varchar(v.clone())),
            other => Ok(Value::Varchar(other.to_string())),
        },
        LogicalType::Boolean => parse_bool(value).map(Value::Boolean),
        LogicalType::TinyInt => parse_i64(value).and_then(|v| {
            i8::try_from(v).map(Value::TinyInt).map_err(|_| {
                paro_error::invalid_parameter(format!("value {} out of range for TINYINT", v))
            })
        }),
        LogicalType::SmallInt => parse_i64(value).and_then(|v| {
            i16::try_from(v).map(Value::SmallInt).map_err(|_| {
                paro_error::invalid_parameter(format!("value {} out of range for SMALLINT", v))
            })
        }),
        LogicalType::Integer => parse_i64(value).and_then(|v| {
            i32::try_from(v).map(Value::Integer).map_err(|_| {
                paro_error::invalid_parameter(format!("value {} out of range for INTEGER", v))
            })
        }),
        LogicalType::BigInt => parse_i64(value).map(Value::BigInt),
        LogicalType::UTinyInt => parse_u64(value).and_then(|v| {
            u8::try_from(v).map(Value::UTinyInt).map_err(|_| {
                paro_error::invalid_parameter(format!("value {} out of range for UTINYINT", v))
            })
        }),
        LogicalType::USmallInt => parse_u64(value).and_then(|v| {
            u16::try_from(v).map(Value::USmallInt).map_err(|_| {
                paro_error::invalid_parameter(format!("value {} out of range for USMALLINT", v))
            })
        }),
        LogicalType::UInteger => parse_u64(value).and_then(|v| {
            u32::try_from(v).map(Value::UInteger).map_err(|_| {
                paro_error::invalid_parameter(format!("value {} out of range for UINTEGER", v))
            })
        }),
        LogicalType::UBigInt => parse_u64(value).map(Value::UBigInt),
        LogicalType::Float => parse_f64(value).map(|v| Value::Float(v as f32)),
        LogicalType::Double => parse_f64(value).map(Value::Double),
        _ => Err(paro_error::not_implemented(format!(
            "read_ndjson does not support type {}",
            target_type
        ))),
    }
}

fn parse_bool(value: &JsonValue) -> Result<bool> {
    match value {
        JsonValue::Bool(v) => Ok(*v),
        JsonValue::String(v) => match v.to_lowercase().as_str() {
            "true" | "t" | "1" => Ok(true),
            "false" | "f" | "0" => Ok(false),
            _ => Err(paro_error::invalid_parameter(format!(
                "invalid boolean value: {}",
                v
            ))),
        },
        _ => Err(paro_error::invalid_parameter(format!(
            "invalid boolean JSON value: {}",
            value
        ))),
    }
}

fn parse_i64(value: &JsonValue) -> Result<i64> {
    match value {
        JsonValue::Number(v) => v
            .as_i64()
            .ok_or_else(|| paro_error::invalid_parameter(format!("invalid integer value: {}", v))),
        JsonValue::String(v) => v
            .parse::<i64>()
            .map_err(|_| paro_error::invalid_parameter(format!("invalid integer value: {}", v))),
        _ => Err(paro_error::invalid_parameter(format!(
            "invalid integer JSON value: {}",
            value
        ))),
    }
}

fn parse_u64(value: &JsonValue) -> Result<u64> {
    match value {
        JsonValue::Number(v) => v.as_u64().ok_or_else(|| {
            paro_error::invalid_parameter(format!("invalid unsigned integer value: {}", v))
        }),
        JsonValue::String(v) => v.parse::<u64>().map_err(|_| {
            paro_error::invalid_parameter(format!("invalid unsigned integer value: {}", v))
        }),
        _ => Err(paro_error::invalid_parameter(format!(
            "invalid unsigned integer JSON value: {}",
            value
        ))),
    }
}

fn parse_f64(value: &JsonValue) -> Result<f64> {
    match value {
        JsonValue::Number(v) => v
            .as_f64()
            .ok_or_else(|| paro_error::invalid_parameter(format!("invalid float value: {}", v))),
        JsonValue::String(v) => v
            .parse::<f64>()
            .map_err(|_| paro_error::invalid_parameter(format!("invalid float value: {}", v))),
        _ => Err(paro_error::invalid_parameter(format!(
            "invalid float JSON value: {}",
            value
        ))),
    }
}

fn extract_schema(value: &Value) -> Result<(Vec<String>, Vec<LogicalType>)> {
    let Value::Struct(values, fields) = value else {
        return Err(paro_error::invalid_parameter(
            "read_ndjson schema must be a STRUCT",
        ));
    };
    if values.len() != fields.len() {
        return Err(paro_error::internal(
            "read_ndjson schema struct length mismatch",
        ));
    }
    let names = fields.iter().map(|(name, _)| name.clone()).collect();
    let types = fields.iter().map(|(_, ty)| ty.clone()).collect();
    Ok((names, types))
}

fn is_option_struct(fields: &[(String, LogicalType)]) -> bool {
    if fields.is_empty() {
        return false;
    }
    fields.iter().all(|(name, _)| {
        matches!(
            name.to_lowercase().as_str(),
            "format"
                | "delimiter"
                | "null"
                | "null_string"
                | "header"
                | "quote"
                | "escape"
                | "parallel"
                | "parallel_workers"
        )
    })
}

fn value_as_string(value: &Value) -> Result<Option<String>> {
    match value {
        Value::Null(_) => Ok(None),
        Value::Varchar(s) => Ok(Some(s.clone())),
        Value::Boolean(v) => Ok(Some(v.to_string())),
        Value::Integer(v) => Ok(Some(v.to_string())),
        Value::BigInt(v) => Ok(Some(v.to_string())),
        _ => Err(paro_error::invalid_parameter(
            "read_ndjson option expects a string",
        )),
    }
}
