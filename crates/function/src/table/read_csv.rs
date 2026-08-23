// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! read_csv Table Function
//!
//!
//!
//! ## Overview
//! Provides a minimal CSV/TEXT reader for COPY FROM.
//! Direct table-function calls bind a file path at runtime. COPY FROM binds an
//! explicit file-or-STDIN source in the planner and carries it with the plan.

use std::any::Any;
use std::fs::File;
use std::io::{BufRead, BufReader, Seek, SeekFrom};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Mutex;

use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use paro_common::vector::VECTOR_SIZE;

use crate::copy::{CopyFormat, CopyFromSource, CopyOptions};
use crate::scalar::cast::array_casts::parse_vector_literal;
use crate::scalar::cast::date_casts::parse_date_text;
use crate::scalar::cast::decimal_casts::parse_decimal_text;

use super::{
    GlobalTableFunctionState, LocalTableFunctionState, TableFunction, TableFunctionBindData,
    TableFunctionBindInput, TableFunctionInitInput, TableFunctionInput, TableFunctionResult,
    TableFunctionSet,
};

const COPY_PARALLEL_SPLIT_MIN_BYTES: u64 = 1_048_576;
const COPY_PARALLEL_MAX_WORKERS: usize = 32;

pub(crate) fn open_copy_reader(
    source: &CopyFromSource,
    input: &TableFunctionInitInput,
) -> Result<Box<dyn BufRead + Send>> {
    match source {
        CopyFromSource::File(path) => open_file_reader(path),
        CopyFromSource::Stdin => Ok(Box::new(BufReader::new(
            input.copy_stdin_source()?.open_reader()?,
        ))),
    }
}

fn open_file_reader(path: &str) -> Result<Box<dyn BufRead + Send>> {
    let file = File::open(path)
        .map_err(|e| paro_error::io_error(format!("Failed to open CSV file '{}': {}", path, e)))?;
    Ok(Box::new(BufReader::new(file)))
}

#[derive(Clone, Debug)]
struct ReadCsvOptions {
    delimiter: String,
    null_string: String,
    header: bool,
    quote: Option<char>,
    escape: Option<char>,
    parallel: bool,
    parallel_workers: Option<usize>,
}

impl ReadCsvOptions {
    fn from_value(value: Option<&Value>) -> Result<Self> {
        let mut format = CopyFormat::Csv;
        let mut delimiter: Option<String> = None;
        let mut null_string: Option<String> = None;
        let mut header: Option<bool> = None;
        let mut quote: Option<char> = None;
        let mut escape: Option<char> = None;
        let mut parallel: Option<bool> = None;
        let mut parallel_workers: Option<usize> = None;

        if let Some(value) = value {
            let Value::Struct(values, fields) = value else {
                return Err(paro_error::invalid_parameter(
                    "read_csv options must be a STRUCT",
                ));
            };
            if values.len() != fields.len() {
                return Err(paro_error::internal(
                    "read_csv options struct length mismatch",
                ));
            }

            for (value, (name, _ty)) in values.iter().zip(fields.iter()) {
                let key = name.to_lowercase();
                match key.as_str() {
                    "format" => {
                        if let Some(v) = value_as_string(value)? {
                            format = parse_format(&v)?;
                        }
                    }
                    "delimiter" => {
                        delimiter = value_as_string(value)?;
                    }
                    "null" | "null_string" => {
                        null_string = value_as_string(value)?;
                    }
                    "header" => {
                        header = value_as_bool(value)?;
                    }
                    "quote" => {
                        if let Some(v) = value_as_string(value)? {
                            quote = Some(parse_char("quote", &v)?);
                        }
                    }
                    "escape" => {
                        if let Some(v) = value_as_string(value)? {
                            escape = Some(parse_char("escape", &v)?);
                        }
                    }
                    "parallel" => {
                        parallel = value_as_bool(value)?;
                    }
                    "parallel_workers" => {
                        parallel_workers = value_as_usize(value)?;
                    }
                    _ => {
                        return Err(paro_error::invalid_parameter(format!(
                            "unknown read_csv option: {}",
                            name
                        )));
                    }
                }
            }
        }

        let header = header.unwrap_or(false);

        let (delimiter, null_string, quote, escape) = match format {
            CopyFormat::Csv => {
                let delimiter = delimiter.unwrap_or_else(|| ",".to_string());
                let null_string = null_string.unwrap_or_else(String::new);
                let quote = quote.or(Some('"')).unwrap_or('"');
                let escape = escape.or(Some(quote)).unwrap_or(quote);
                (delimiter, null_string, Some(quote), Some(escape))
            }
            CopyFormat::Text => {
                let delimiter = delimiter.unwrap_or_else(|| "\t".to_string());
                let null_string = null_string.unwrap_or_else(|| "\\N".to_string());
                (delimiter, null_string, None, None)
            }
            CopyFormat::Binary => {
                return Err(paro_error::not_implemented(
                    "read_csv does not support BINARY format",
                ));
            }
            CopyFormat::Ndjson => {
                return Err(paro_error::not_implemented(
                    "read_csv does not support NDJSON format",
                ));
            }
        };

        if delimiter.is_empty() {
            return Err(paro_error::invalid_parameter(
                "read_csv delimiter cannot be empty",
            ));
        }
        if delimiter.chars().count() != 1 {
            return Err(paro_error::invalid_parameter(
                "read_csv delimiter must be a single character",
            ));
        }

        if matches!(format, CopyFormat::Csv) && (quote.is_none() || escape.is_none()) {
            return Err(paro_error::invalid_parameter(
                "CSV format requires quote and escape characters",
            ));
        }

        Ok(Self {
            delimiter,
            null_string,
            header,
            quote,
            escape,
            parallel: parallel.unwrap_or(false),
            parallel_workers,
        })
    }

    fn from_copy_options(options: &CopyOptions) -> Result<Self> {
        if !matches!(options.format, CopyFormat::Csv | CopyFormat::Text) {
            return Err(paro_error::invalid_parameter(
                "read_csv COPY input requires CSV or TEXT format",
            ));
        }

        let delimiter = options
            .delimiter()
            .expect("CSV/TEXT options always have a delimiter")
            .to_string();
        if delimiter.chars().count() != 1 {
            return Err(paro_error::invalid_parameter(
                "COPY delimiter must be a single character",
            ));
        }

        let null_string = options
            .null_string()
            .expect("CSV/TEXT options always have a NULL marker")
            .to_string();
        let (quote, escape) = match options.format {
            CopyFormat::Csv => (options.quote(), options.escape()),
            CopyFormat::Text => (None, None),
            CopyFormat::Binary | CopyFormat::Ndjson => unreachable!("format checked above"),
        };

        Ok(Self {
            delimiter,
            null_string,
            header: options.header(),
            quote,
            escape,
            parallel: options.parallel,
            parallel_workers: options.parallel_workers,
        })
    }
}

#[derive(Clone, Debug)]
struct ReadCsvBindData {
    source: CopyFromSource,
    options: ReadCsvOptions,
    types: Vec<LogicalType>,
    skip_header: bool,
}

impl TableFunctionBindData for ReadCsvBindData {
    fn clone_box(&self) -> Box<dyn TableFunctionBindData> {
        Box::new(self.clone())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[derive(Debug, Clone)]
struct ReadCsvPartition {
    start: u64,
    end: u64,
    skip_header: bool,
}

#[derive(Debug)]
struct ReadCsvParallelState {
    file_path: String,
    partitions: Vec<ReadCsvPartition>,
    next_partition: AtomicUsize,
}

struct ReadCsvGlobalState {
    mode: ReadCsvExecutionMode,
}

enum ReadCsvExecutionMode {
    Serial {
        reader: Mutex<ReadCsvReader>,
        finished: AtomicBool,
    },
    Parallel(ReadCsvParallelState),
}

struct ReadCsvReader {
    reader: Box<dyn BufRead + Send>,
    row_number: usize,
}

impl GlobalTableFunctionState for ReadCsvGlobalState {
    fn max_threads(&self) -> usize {
        match &self.mode {
            ReadCsvExecutionMode::Serial { .. } => 1,
            ReadCsvExecutionMode::Parallel(state) => state.partitions.len().max(1),
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

#[derive(Default)]
struct ReadCsvLocalState {
    record_buffer: String,
    partition_reader: Option<ReadCsvPartitionReader>,
}

struct ReadCsvPartitionReader {
    reader: BufReader<File>,
    row_number: usize,
    end_offset: u64,
}

impl LocalTableFunctionState for ReadCsvLocalState {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

pub fn create_read_csv_function() -> TableFunction {
    TableFunction::new("read_csv", vec![LogicalType::Varchar])
        .with_bind(read_csv_bind)
        .with_init_global(read_csv_init_global)
        .with_init_local(read_csv_init_local)
        .with_function(read_csv_function)
}

pub fn create_read_csv_function_set() -> TableFunctionSet {
    let mut set = TableFunctionSet::new("read_csv");
    set.add_function(create_read_csv_function());
    set
}

fn read_csv_bind(
    input: &TableFunctionBindInput,
    return_types: &mut Vec<LogicalType>,
    return_names: &mut Vec<String>,
) -> Result<Option<Box<dyn TableFunctionBindData>>> {
    let path_value = input
        .inputs
        .get(0)
        .ok_or_else(|| paro_error::invalid_parameter("read_csv expects a file path argument"))?;
    let file_path = match path_value {
        Value::Varchar(path) => path.clone(),
        _ => {
            return Err(paro_error::invalid_parameter(
                "read_csv path must be VARCHAR",
            ))
        }
    };

    let mut schema: Option<(Vec<String>, Vec<LogicalType>)> = None;
    let mut options_value: Option<&Value> = None;

    if let Some(value) = input.inputs.get(2) {
        let schema_value = input
            .inputs
            .get(1)
            .ok_or_else(|| paro_error::invalid_parameter("read_csv schema argument missing"))?;
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

    let options = ReadCsvOptions::from_value(options_value)?;

    let (names, types) = match schema {
        Some(schema) => schema,
        None => infer_schema(&file_path, &options)?,
    };

    return_types.extend(types.iter().cloned());
    return_names.extend(names.iter().cloned());

    let bind_data = ReadCsvBindData {
        source: CopyFromSource::File(file_path),
        options: options.clone(),
        types,
        skip_header: options.header,
    };

    Ok(Some(Box::new(bind_data)))
}

pub(crate) fn bind_copy_from(
    source: CopyFromSource,
    options: &CopyOptions,
    names: &[String],
    types: &[LogicalType],
) -> Result<Box<dyn TableFunctionBindData>> {
    if names.len() != types.len() {
        return Err(paro_error::invalid_input(
            "COPY FROM input names/types length mismatch",
        ));
    }
    let options = ReadCsvOptions::from_copy_options(options)?;
    let skip_header = options.header;
    Ok(Box::new(ReadCsvBindData {
        source,
        options,
        types: types.to_vec(),
        skip_header,
    }))
}

fn read_csv_init_global(
    input: &TableFunctionInitInput,
) -> Result<Option<Box<dyn GlobalTableFunctionState>>> {
    let bind_data = input
        .bind_data
        .and_then(|data| data.as_any().downcast_ref::<ReadCsvBindData>())
        .ok_or_else(|| paro_error::internal("Invalid read_csv bind data".to_string()))?;

    let max_threads_hint = input.max_threads_hint.max(1);
    let configured_workers = bind_data
        .options
        .parallel_workers
        .unwrap_or(max_threads_hint);
    let requested_workers = configured_workers
        .min(max_threads_hint)
        .min(COPY_PARALLEL_MAX_WORKERS)
        .max(1);

    let can_parallel = bind_data.options.parallel
        && matches!(&bind_data.source, CopyFromSource::File(_))
        && requested_workers > 1;

    if can_parallel {
        let CopyFromSource::File(file_path) = &bind_data.source else {
            unreachable!("parallel COPY source must be a file")
        };
        let file_size = File::open(file_path)
            .map_err(|e| {
                paro_error::io_error(format!("Failed to open CSV file '{}': {}", file_path, e))
            })?
            .metadata()
            .map_err(|e| {
                paro_error::io_error(format!("Failed to stat CSV file '{}': {}", file_path, e))
            })?
            .len();
        let meets_size_threshold = file_size >= COPY_PARALLEL_SPLIT_MIN_BYTES
            || bind_data.options.parallel_workers.is_some();

        if meets_size_threshold {
            let partitions =
                build_file_partitions(file_path, requested_workers, bind_data.skip_header)?;
            if partitions.len() > 1 {
                let state = ReadCsvGlobalState {
                    mode: ReadCsvExecutionMode::Parallel(ReadCsvParallelState {
                        file_path: file_path.clone(),
                        partitions,
                        next_partition: AtomicUsize::new(0),
                    }),
                };
                return Ok(Some(Box::new(state)));
            }
        }
    }

    let reader = open_copy_reader(&bind_data.source, input)?;
    let state = ReadCsvGlobalState {
        mode: ReadCsvExecutionMode::Serial {
            reader: Mutex::new(ReadCsvReader {
                reader,
                row_number: 0,
            }),
            finished: AtomicBool::new(false),
        },
    };

    if bind_data.skip_header {
        let ReadCsvExecutionMode::Serial { reader, finished } = &state.mode else {
            unreachable!("read_csv serial state expected");
        };
        let mut guard = reader
            .lock()
            .map_err(|e| paro_error::internal(e.to_string()))?;
        if read_record(&mut guard, &bind_data.options, &mut String::new())?.is_none() {
            finished.store(true, Ordering::SeqCst);
        }
    }

    Ok(Some(Box::new(state)))
}

fn read_csv_init_local(
    _input: &TableFunctionInitInput,
    _global_state: Option<&dyn GlobalTableFunctionState>,
) -> Result<Option<Box<dyn LocalTableFunctionState>>> {
    Ok(Some(Box::new(ReadCsvLocalState::default())))
}

fn read_csv_function(
    input: &mut TableFunctionInput,
    output: &mut Chunk,
) -> Result<TableFunctionResult> {
    let bind_data = input
        .bind_data
        .and_then(|data| data.as_any().downcast_ref::<ReadCsvBindData>())
        .ok_or_else(|| paro_error::internal("Invalid read_csv bind data".to_string()))?;

    let global_state = input
        .global_state
        .and_then(|state| state.as_any().downcast_ref::<ReadCsvGlobalState>())
        .ok_or_else(|| paro_error::internal("Invalid read_csv global state".to_string()))?;

    let local_state = input
        .local_state
        .as_mut()
        .and_then(|state| state.as_any_mut().downcast_mut::<ReadCsvLocalState>())
        .ok_or_else(|| paro_error::internal("Invalid read_csv local state".to_string()))?;

    match &global_state.mode {
        ReadCsvExecutionMode::Serial { reader, finished } => {
            if finished.load(Ordering::SeqCst) {
                output.set_cardinality(0);
                return Ok(TableFunctionResult::Finished);
            }

            let mut guard = reader
                .lock()
                .map_err(|e| paro_error::internal(e.to_string()))?;
            let mut record_buffer = std::mem::take(&mut local_state.record_buffer);

            let mut produced = 0;
            let capacity = output.capacity().min(VECTOR_SIZE);
            while produced < capacity {
                let record = read_record(&mut guard, &bind_data.options, &mut record_buffer)?;
                let Some(fields) = record else {
                    finished.store(true, Ordering::SeqCst);
                    break;
                };

                if fields.len() != bind_data.types.len() {
                    return Err(paro_error::invalid_value(
                        "CSV row has incorrect column count".to_string(),
                        format!(
                            "expected {}, got {} at row {}",
                            bind_data.types.len(),
                            fields.len(),
                            guard.row_number
                        ),
                    ));
                }

                for (col_idx, field) in fields.iter().enumerate() {
                    let value = parse_field(field, &bind_data.types[col_idx])?;
                    let col = output.column_mut(col_idx).ok_or_else(|| {
                        paro_error::internal("Output column not found".to_string())
                    })?;
                    col.set_value(produced, &value);
                }

                produced += 1;
            }

            output.set_cardinality(produced);
            local_state.record_buffer = record_buffer;

            if finished.load(Ordering::SeqCst) {
                Ok(TableFunctionResult::Finished)
            } else {
                Ok(TableFunctionResult::HaveMoreOutput)
            }
        }
        ReadCsvExecutionMode::Parallel(parallel_state) => {
            read_csv_parallel_function(bind_data, parallel_state, local_state, output)
        }
    }
}

fn read_csv_parallel_function(
    bind_data: &ReadCsvBindData,
    parallel_state: &ReadCsvParallelState,
    local_state: &mut ReadCsvLocalState,
    output: &mut Chunk,
) -> Result<TableFunctionResult> {
    let mut produced = 0;
    let capacity = output.capacity().min(VECTOR_SIZE);

    while produced < capacity {
        if local_state.partition_reader.is_none()
            && !assign_next_partition(parallel_state, &bind_data.options, local_state)?
        {
            break;
        }

        let (record, row_number) = {
            let partition_reader = local_state.partition_reader.as_mut().ok_or_else(|| {
                paro_error::internal("read_csv partition reader missing".to_string())
            })?;
            let row = read_record_partition(
                partition_reader,
                &bind_data.options,
                &mut local_state.record_buffer,
            )?;
            let row_number = partition_reader.row_number;
            (row, row_number)
        };

        let Some(fields) = record else {
            local_state.partition_reader = None;
            continue;
        };

        if fields.len() != bind_data.types.len() {
            return Err(paro_error::invalid_value(
                "CSV row has incorrect column count".to_string(),
                format!(
                    "expected {}, got {} at row {}",
                    bind_data.types.len(),
                    fields.len(),
                    row_number
                ),
            ));
        }

        for (col_idx, field) in fields.iter().enumerate() {
            let value = parse_field(field, &bind_data.types[col_idx])?;
            let col = output
                .column_mut(col_idx)
                .ok_or_else(|| paro_error::internal("Output column not found".to_string()))?;
            col.set_value(produced, &value);
        }

        produced += 1;
    }

    output.set_cardinality(produced);

    let done = local_state.partition_reader.is_none()
        && parallel_state.next_partition.load(Ordering::SeqCst) >= parallel_state.partitions.len();

    if done {
        Ok(TableFunctionResult::Finished)
    } else {
        Ok(TableFunctionResult::HaveMoreOutput)
    }
}

fn assign_next_partition(
    parallel_state: &ReadCsvParallelState,
    options: &ReadCsvOptions,
    local_state: &mut ReadCsvLocalState,
) -> Result<bool> {
    loop {
        let partition_idx = parallel_state.next_partition.fetch_add(1, Ordering::SeqCst);
        if partition_idx >= parallel_state.partitions.len() {
            return Ok(false);
        }
        let partition = &parallel_state.partitions[partition_idx];
        if partition.start >= partition.end {
            continue;
        }

        let mut file = File::open(&parallel_state.file_path).map_err(|e| {
            paro_error::io_error(format!(
                "Failed to open CSV file '{}': {}",
                parallel_state.file_path, e
            ))
        })?;
        file.seek(SeekFrom::Start(partition.start)).map_err(|e| {
            paro_error::io_error(format!(
                "Failed to seek CSV file '{}': {}",
                parallel_state.file_path, e
            ))
        })?;

        let mut partition_reader = ReadCsvPartitionReader {
            reader: BufReader::new(file),
            row_number: 0,
            end_offset: partition.end,
        };

        if partition.skip_header {
            let _ = read_record_partition(&mut partition_reader, options, &mut String::new())?;
        }

        local_state.partition_reader = Some(partition_reader);
        return Ok(true);
    }
}

fn read_record_partition(
    reader: &mut ReadCsvPartitionReader,
    options: &ReadCsvOptions,
    buffer: &mut String,
) -> Result<Option<Vec<ParsedField>>> {
    buffer.clear();

    loop {
        let current_pos = reader
            .reader
            .stream_position()
            .map_err(|e| paro_error::io_error(format!("Failed to read CSV: {}", e)))?;
        if current_pos >= reader.end_offset {
            if buffer.is_empty() {
                return Ok(None);
            }
            break;
        }

        let mut line = String::new();
        let bytes = reader
            .reader
            .read_line(&mut line)
            .map_err(|e| paro_error::io_error(format!("Failed to read CSV: {}", e)))?;
        if bytes == 0 {
            if buffer.is_empty() {
                return Ok(None);
            }
            break;
        }

        buffer.push_str(&line);
        if record_complete(buffer, options.quote, options.escape) {
            break;
        }
    }

    trim_line_endings(buffer);
    reader.row_number += 1;

    let fields = parse_record_fields(buffer, options)?;
    Ok(Some(fields))
}

fn build_file_partitions(
    file_path: &str,
    requested_workers: usize,
    skip_header: bool,
) -> Result<Vec<ReadCsvPartition>> {
    let file_size = File::open(file_path)
        .map_err(|e| {
            paro_error::io_error(format!("Failed to open CSV file '{}': {}", file_path, e))
        })?
        .metadata()
        .map_err(|e| {
            paro_error::io_error(format!("Failed to stat CSV file '{}': {}", file_path, e))
        })?
        .len();

    if file_size == 0 {
        return Ok(vec![ReadCsvPartition {
            start: 0,
            end: 0,
            skip_header,
        }]);
    }

    let worker_count = requested_workers
        .max(1)
        .min(COPY_PARALLEL_MAX_WORKERS)
        .min(file_size as usize);
    if worker_count <= 1 {
        return Ok(vec![ReadCsvPartition {
            start: 0,
            end: file_size,
            skip_header,
        }]);
    }

    let mut partitions = Vec::with_capacity(worker_count);
    let mut previous_end = 0_u64;
    for idx in 0..worker_count {
        let mut start = if idx == 0 {
            0
        } else {
            align_to_next_line(
                file_path,
                (idx as u64 * file_size) / worker_count as u64,
                file_size,
            )?
        };
        let mut end = if idx + 1 == worker_count {
            file_size
        } else {
            align_to_next_line(
                file_path,
                ((idx + 1) as u64 * file_size) / worker_count as u64,
                file_size,
            )?
        };

        if start < previous_end {
            start = previous_end;
        }
        if end < start {
            end = start;
        }
        if start < end {
            partitions.push(ReadCsvPartition {
                start,
                end,
                skip_header: false,
            });
            previous_end = end;
        }
    }

    if partitions.is_empty() {
        partitions.push(ReadCsvPartition {
            start: 0,
            end: file_size,
            skip_header: false,
        });
    }
    if skip_header {
        if let Some(first) = partitions.first_mut() {
            first.skip_header = true;
        }
    }
    Ok(partitions)
}

fn align_to_next_line(file_path: &str, offset: u64, file_size: u64) -> Result<u64> {
    if offset == 0 {
        return Ok(0);
    }
    if offset >= file_size {
        return Ok(file_size);
    }

    let mut file = File::open(file_path).map_err(|e| {
        paro_error::io_error(format!("Failed to open CSV file '{}': {}", file_path, e))
    })?;
    file.seek(SeekFrom::Start(offset)).map_err(|e| {
        paro_error::io_error(format!("Failed to seek CSV file '{}': {}", file_path, e))
    })?;
    let mut reader = BufReader::new(file);

    let mut line = String::new();
    let bytes = reader.read_line(&mut line).map_err(|e| {
        paro_error::io_error(format!("Failed to read CSV file '{}': {}", file_path, e))
    })?;
    if bytes == 0 {
        return Ok(file_size);
    }

    reader.stream_position().map_err(|e| {
        paro_error::io_error(format!("Failed to read CSV file '{}': {}", file_path, e))
    })
}

#[derive(Debug)]
struct ParsedField {
    value: String,
    is_null: bool,
}

fn read_record(
    reader: &mut ReadCsvReader,
    options: &ReadCsvOptions,
    buffer: &mut String,
) -> Result<Option<Vec<ParsedField>>> {
    buffer.clear();

    loop {
        let mut line = String::new();
        let bytes = reader
            .reader
            .read_line(&mut line)
            .map_err(|e| paro_error::io_error(format!("Failed to read CSV: {}", e)))?;
        if bytes == 0 {
            if buffer.is_empty() {
                return Ok(None);
            }
            break;
        }
        buffer.push_str(&line);
        if record_complete(buffer, options.quote, options.escape) {
            break;
        }
    }

    trim_line_endings(buffer);
    reader.row_number += 1;

    let fields = parse_record_fields(buffer, options)?;
    Ok(Some(fields))
}

fn record_complete(record: &str, quote: Option<char>, escape: Option<char>) -> bool {
    let Some(quote) = quote else {
        return true;
    };
    let escape = escape.unwrap_or(quote);

    let mut in_quotes = false;
    let mut iter = record.chars().peekable();
    while let Some(ch) = iter.next() {
        if ch == quote {
            if in_quotes {
                if escape == quote {
                    if let Some(next) = iter.peek() {
                        if *next == quote {
                            iter.next();
                            continue;
                        }
                    }
                }
                in_quotes = false;
            } else {
                in_quotes = true;
            }
            continue;
        }
        if in_quotes && ch == escape && escape != quote {
            iter.next();
        }
    }
    !in_quotes
}

fn trim_line_endings(record: &mut String) {
    while record.ends_with('\n') || record.ends_with('\r') {
        record.pop();
    }
}

fn parse_record_fields(record: &str, options: &ReadCsvOptions) -> Result<Vec<ParsedField>> {
    if options.quote.is_none() {
        return parse_text_fields(record, options);
    }
    parse_csv_fields(record, options)
}

fn parse_csv_fields(record: &str, options: &ReadCsvOptions) -> Result<Vec<ParsedField>> {
    let delimiter = options
        .delimiter
        .chars()
        .next()
        .ok_or_else(|| paro_error::invalid_parameter("Delimiter cannot be empty"))?;
    let quote = options.quote;
    let escape = options.escape.or(quote);

    let mut fields = Vec::new();
    let mut current = String::new();
    let mut in_quotes = false;
    let mut quoted = false;

    let mut iter = record.chars().peekable();
    while let Some(ch) = iter.next() {
        if in_quotes {
            if Some(ch) == quote {
                if escape == quote {
                    if let Some(next) = iter.peek() {
                        if Some(*next) == quote {
                            current.push(*next);
                            iter.next();
                            continue;
                        }
                    }
                }
                in_quotes = false;
                continue;
            }
            if Some(ch) == escape && escape != quote {
                if let Some(next) = iter.next() {
                    current.push(next);
                    continue;
                }
            }
            current.push(ch);
            continue;
        }

        if ch == delimiter {
            let value = std::mem::take(&mut current);
            let is_null = !quoted && value == options.null_string;
            fields.push(ParsedField { value, is_null });
            quoted = false;
            continue;
        }

        if Some(ch) == quote && current.is_empty() {
            in_quotes = true;
            quoted = true;
            continue;
        }

        current.push(ch);
    }

    if in_quotes {
        return Err(paro_error::invalid_parameter(
            "Unterminated quote in CSV record",
        ));
    }

    let is_null = !quoted && current == options.null_string;
    fields.push(ParsedField {
        value: current,
        is_null,
    });

    Ok(fields)
}

fn parse_text_fields(record: &str, options: &ReadCsvOptions) -> Result<Vec<ParsedField>> {
    let delimiter = options
        .delimiter
        .chars()
        .next()
        .ok_or_else(|| paro_error::invalid_parameter("Delimiter cannot be empty"))?;

    let mut fields = Vec::new();
    let mut current = String::new();
    let mut iter = record.chars().peekable();

    while let Some(ch) = iter.next() {
        if ch == '\\' {
            current.push(ch);
            if let Some(next) = iter.next() {
                current.push(next);
            }
            continue;
        }

        if ch == delimiter {
            fields.push(parse_text_field(std::mem::take(&mut current), options));
            continue;
        }

        current.push(ch);
    }

    fields.push(parse_text_field(current, options));
    Ok(fields)
}

fn parse_text_field(raw: String, options: &ReadCsvOptions) -> ParsedField {
    if raw == options.null_string {
        return ParsedField {
            value: raw,
            is_null: true,
        };
    }

    ParsedField {
        value: decode_text_escapes(&raw),
        is_null: false,
    }
}

fn decode_text_escapes(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len());
    let mut iter = raw.chars().peekable();

    while let Some(ch) = iter.next() {
        if ch != '\\' {
            out.push(ch);
            continue;
        }

        let Some(escaped) = iter.next() else {
            out.push('\\');
            break;
        };

        match escaped {
            '0'..='7' => {
                let mut value = escaped.to_digit(8).unwrap_or(0);
                for _ in 0..2 {
                    let Some(next) = iter.peek().copied() else {
                        break;
                    };
                    let Some(digit) = next.to_digit(8) else {
                        break;
                    };
                    iter.next();
                    value = (value << 3) + digit;
                }
                out.push(char::from_u32(value & 0xff).unwrap_or('\0'));
            }
            'x' => {
                let Some(first) = iter.peek().copied().and_then(|c| c.to_digit(16)) else {
                    out.push('x');
                    continue;
                };
                iter.next();
                let mut value = first;
                if let Some(second) = iter.peek().copied().and_then(|c| c.to_digit(16)) {
                    iter.next();
                    value = (value << 4) + second;
                }
                out.push(char::from_u32(value & 0xff).unwrap_or('\0'));
            }
            'b' => out.push('\u{0008}'),
            'f' => out.push('\u{000c}'),
            'n' => out.push('\n'),
            'r' => out.push('\r'),
            't' => out.push('\t'),
            'v' => out.push('\u{000b}'),
            other => out.push(other),
        }
    }

    out
}

fn infer_schema(path: &str, options: &ReadCsvOptions) -> Result<(Vec<String>, Vec<LogicalType>)> {
    let mut reader = ReadCsvReader {
        reader: open_file_reader(path)?,
        row_number: 0,
    };
    let mut buffer = String::new();
    let Some(fields) = read_record(&mut reader, options, &mut buffer)? else {
        return Err(paro_error::invalid_parameter(
            "read_csv cannot infer schema from an empty file",
        ));
    };

    let names: Vec<String> = if options.header {
        fields.iter().map(|f| f.value.clone()).collect()
    } else {
        (0..fields.len())
            .map(|idx| format!("column{}", idx + 1))
            .collect()
    };
    let types = vec![LogicalType::Varchar; fields.len()];
    Ok((names, types))
}

fn extract_schema(value: &Value) -> Result<(Vec<String>, Vec<LogicalType>)> {
    let Value::Struct(values, fields) = value else {
        return Err(paro_error::invalid_parameter(
            "read_csv schema must be a STRUCT",
        ));
    };
    if values.len() != fields.len() {
        return Err(paro_error::internal(
            "read_csv schema struct length mismatch",
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

fn parse_format(value: &str) -> Result<CopyFormat> {
    match value.to_lowercase().as_str() {
        "csv" => Ok(CopyFormat::Csv),
        "text" => Ok(CopyFormat::Text),
        "binary" => Ok(CopyFormat::Binary),
        "ndjson" | "json" => Ok(CopyFormat::Ndjson),
        _ => Err(paro_error::invalid_parameter(format!(
            "Unknown COPY format: {}",
            value
        ))),
    }
}

fn parse_char(key: &str, value: &str) -> Result<char> {
    let mut chars = value.chars();
    let Some(ch) = chars.next() else {
        return Err(paro_error::invalid_parameter(format!(
            "read_csv option {} expects a single character",
            key
        )));
    };
    if chars.next().is_some() {
        return Err(paro_error::invalid_parameter(format!(
            "read_csv option {} expects a single character",
            key
        )));
    }
    Ok(ch)
}

fn value_as_string(value: &Value) -> Result<Option<String>> {
    match value {
        Value::Null(_) => Ok(None),
        Value::Varchar(s) => Ok(Some(s.clone())),
        Value::Boolean(v) => Ok(Some(v.to_string())),
        Value::Integer(v) => Ok(Some(v.to_string())),
        Value::BigInt(v) => Ok(Some(v.to_string())),
        _ => Err(paro_error::invalid_parameter(
            "read_csv option expects a string",
        )),
    }
}

fn value_as_bool(value: &Value) -> Result<Option<bool>> {
    match value {
        Value::Null(_) => Ok(None),
        Value::Boolean(v) => Ok(Some(*v)),
        Value::Varchar(s) => match s.to_lowercase().as_str() {
            "true" | "t" | "1" => Ok(Some(true)),
            "false" | "f" | "0" => Ok(Some(false)),
            _ => Err(paro_error::invalid_parameter(
                "read_csv option expects a boolean value",
            )),
        },
        _ => Err(paro_error::invalid_parameter(
            "read_csv option expects a boolean value",
        )),
    }
}

fn value_as_usize(value: &Value) -> Result<Option<usize>> {
    match value {
        Value::Null(_) => Ok(None),
        Value::Integer(v) => {
            if *v < 1 {
                return Err(paro_error::invalid_parameter(
                    "read_csv parallel_workers expects a value >= 1",
                ));
            }
            Ok(Some(*v as usize))
        }
        Value::BigInt(v) => {
            if *v < 1 {
                return Err(paro_error::invalid_parameter(
                    "read_csv parallel_workers expects a value >= 1",
                ));
            }
            Ok(Some(*v as usize))
        }
        Value::Varchar(v) => {
            let parsed = v.parse::<usize>().map_err(|_| {
                paro_error::invalid_parameter(
                    "read_csv parallel_workers expects a positive integer value",
                )
            })?;
            if parsed < 1 {
                return Err(paro_error::invalid_parameter(
                    "read_csv parallel_workers expects a value >= 1",
                ));
            }
            Ok(Some(parsed))
        }
        _ => Err(paro_error::invalid_parameter(
            "read_csv parallel_workers expects a positive integer value",
        )),
    }
}

fn parse_field(field: &ParsedField, target_type: &LogicalType) -> Result<Value> {
    if field.is_null {
        return Ok(Value::Null(target_type.clone()));
    }

    match target_type {
        LogicalType::Varchar
        | LogicalType::VarcharCollation(_)
        | LogicalType::Json
        | LogicalType::Jsonb
        | LogicalType::TsVector
        | LogicalType::TsQuery => Ok(Value::Varchar(field.value.clone())),
        LogicalType::Boolean => parse_bool(&field.value).map(Value::Boolean),
        LogicalType::TinyInt => parse_i64(&field.value).and_then(|v| {
            i8::try_from(v).map(Value::TinyInt).map_err(|_| {
                paro_error::invalid_value(target_type.to_string(), field.value.clone())
            })
        }),
        LogicalType::SmallInt => parse_i64(&field.value).and_then(|v| {
            i16::try_from(v).map(Value::SmallInt).map_err(|_| {
                paro_error::invalid_value(target_type.to_string(), field.value.clone())
            })
        }),
        LogicalType::Integer => parse_i64(&field.value).and_then(|v| {
            i32::try_from(v).map(Value::Integer).map_err(|_| {
                paro_error::invalid_value(target_type.to_string(), field.value.clone())
            })
        }),
        LogicalType::BigInt => parse_i64(&field.value).map(Value::BigInt),
        LogicalType::UTinyInt => parse_u64(&field.value).and_then(|v| {
            u8::try_from(v).map(Value::UTinyInt).map_err(|_| {
                paro_error::invalid_value(target_type.to_string(), field.value.clone())
            })
        }),
        LogicalType::USmallInt => parse_u64(&field.value).and_then(|v| {
            u16::try_from(v).map(Value::USmallInt).map_err(|_| {
                paro_error::invalid_value(target_type.to_string(), field.value.clone())
            })
        }),
        LogicalType::UInteger => parse_u64(&field.value).and_then(|v| {
            u32::try_from(v).map(Value::UInteger).map_err(|_| {
                paro_error::invalid_value(target_type.to_string(), field.value.clone())
            })
        }),
        LogicalType::UBigInt => parse_u64(&field.value).map(Value::UBigInt),
        LogicalType::Float => parse_f64(&field.value).map(|v| Value::Float(v as f32)),
        LogicalType::Double => parse_f64(&field.value).map(Value::Double),
        LogicalType::Array(child, size) if child.as_ref() == &LogicalType::Float => {
            parse_float_array(&field.value, *size)
        }
        LogicalType::Decimal { precision, scale } => {
            parse_decimal_text(&field.value, *precision, *scale)
        }
        LogicalType::Date => parse_date_text(&field.value)
            .map(Value::Date)
            .ok_or_else(|| paro_error::invalid_value(target_type.to_string(), field.value.clone())),
        _ => Err(paro_error::not_implemented(format!(
            "read_csv does not support type {}",
            target_type
        ))),
    }
}

fn parse_bool(value: &str) -> Result<bool> {
    match value.to_lowercase().as_str() {
        "true" | "t" | "1" => Ok(true),
        "false" | "f" | "0" => Ok(false),
        _ => Err(paro_error::invalid_value(
            LogicalType::Boolean.to_string(),
            value.to_string(),
        )),
    }
}

fn parse_i64(value: &str) -> Result<i64> {
    value
        .parse::<i64>()
        .map_err(|_| paro_error::invalid_value(LogicalType::BigInt.to_string(), value.to_string()))
}

fn parse_u64(value: &str) -> Result<u64> {
    value
        .parse::<u64>()
        .map_err(|_| paro_error::invalid_value(LogicalType::UBigInt.to_string(), value.to_string()))
}

fn parse_f64(value: &str) -> Result<f64> {
    value
        .parse::<f64>()
        .map_err(|_| paro_error::invalid_value(LogicalType::Double.to_string(), value.to_string()))
}

fn parse_float_array(value: &str, size: usize) -> Result<Value> {
    let values = parse_vector_literal(value)?;
    if values.len() != size {
        return Err(paro_error::invalid_value(format!("FLOAT[{size}]"), value));
    }

    let elements = values.into_iter().map(Value::Float).collect();
    Ok(Value::Array(elements, LogicalType::Float, size))
}

#[cfg(test)]
mod tests {
    use super::super::CopyStdinSource;
    use super::*;
    use std::io::Read;
    use std::sync::Arc;

    struct TestCopyStdinSource(Vec<u8>);

    impl CopyStdinSource for TestCopyStdinSource {
        fn open_reader(self: Arc<Self>) -> Result<Box<dyn std::io::Read + Send>> {
            Ok(Box::new(std::io::Cursor::new(self.0.clone())))
        }
    }

    #[test]
    fn copy_stdin_reader_owns_its_statement_source() {
        let mut reader = {
            let runtime = super::super::TestTableFunctionRuntimeContext::with_copy_stdin_source(
                Arc::new(TestCopyStdinSource(b"1,alpha\n2,beta\n".to_vec())),
            );
            let input = TableFunctionInitInput::new(&runtime, None, &[]);
            open_copy_reader(&CopyFromSource::Stdin, &input).unwrap()
        };

        let mut contents = String::new();
        reader.read_to_string(&mut contents).unwrap();
        assert_eq!(contents, "1,alpha\n2,beta\n");
    }

    #[test]
    fn copy_stdin_reader_requires_statement_input() {
        let input = TableFunctionInitInput::new_for_test(None, &[]);
        let err = open_copy_reader(&CopyFromSource::Stdin, &input)
            .err()
            .expect("missing statement input must fail");

        assert!(err.message().contains("requires statement-scoped input"));
    }

    #[test]
    fn parse_field_uses_sql_decimal_cast_semantics() {
        let field = ParsedField {
            value: "-12.345".to_string(),
            is_null: false,
        };

        let value = parse_field(
            &field,
            &LogicalType::Decimal {
                precision: 6,
                scale: 2,
            },
        )
        .unwrap();

        assert_eq!(value, Value::Decimal(-1235, 6, 2));
    }

    #[test]
    fn parse_field_rejects_decimal_precision_overflow() {
        let field = ParsedField {
            value: "1000.00".to_string(),
            is_null: false,
        };

        let err = parse_field(
            &field,
            &LogicalType::Decimal {
                precision: 5,
                scale: 2,
            },
        )
        .unwrap_err();

        assert!(err.message().contains("exceeds precision 5"));
    }

    #[test]
    fn parse_field_supports_date() {
        let field = ParsedField {
            value: "1998-12-01".to_string(),
            is_null: false,
        };

        let value = parse_field(&field, &LogicalType::Date).unwrap();

        assert_eq!(value, Value::Date(10_561));
    }

    #[test]
    fn parse_field_supports_pgvector_text() {
        let field = ParsedField {
            value: "[1.25,-2,3e-2]".to_string(),
            is_null: false,
        };

        let value =
            parse_field(&field, &LogicalType::Array(Box::new(LogicalType::Float), 3)).unwrap();

        assert_eq!(
            value,
            Value::Array(
                vec![Value::Float(1.25), Value::Float(-2.0), Value::Float(0.03)],
                LogicalType::Float,
                3,
            )
        );
    }

    #[test]
    fn parse_field_rejects_pgvector_dimension_mismatch() {
        let field = ParsedField {
            value: "[1,2]".to_string(),
            is_null: false,
        };

        let err =
            parse_field(&field, &LogicalType::Array(Box::new(LogicalType::Float), 3)).unwrap_err();

        assert!(err.message().contains("FLOAT[3]"));
    }
}
