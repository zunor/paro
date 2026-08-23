// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! PostgreSQL binary COPY reader.

use std::any::Any;
use std::io::{BufReader, Read};
use std::sync::Mutex;

use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::types::LogicalType;
use paro_common::vector::{ArrayVector, Vector, VECTOR_SIZE};

use crate::copy::CopyFromSource;
use paro_common::pg_binary::{
    decode_binary_value, is_binary_recv_supported, pg_date_to_unix_days,
    pg_timestamp_to_unix_micros, BinaryInput,
};

use super::{
    GlobalTableFunctionState, LocalTableFunctionState, TableFunction, TableFunctionBindData,
    TableFunctionInitInput, TableFunctionInput, TableFunctionResult,
};

const BINARY_SIGNATURE: &[u8; 11] = b"PGCOPY\n\xff\r\n\0";
const BINARY_HEADER_LEN: usize = 19;
const OIDS_FLAG: u32 = 1 << 16;
const MIN_BINARY_COPY_BATCH_BYTES: usize = 64 * 1024;

#[derive(Debug, Clone)]
struct ReadBinaryBindData {
    source: CopyFromSource,
    types: Vec<LogicalType>,
    decoders: Vec<BinaryColumnDecoder>,
}

#[derive(Debug, Clone)]
enum BinaryColumnDecoder {
    Direct {
        expected_type: LogicalType,
        expected_width: usize,
        decode_batch: BinaryBatchDecoder,
    },
    FloatArray {
        dimension: usize,
    },
    Generic {
        ty: LogicalType,
        expected_width: Option<usize>,
    },
}

type BinaryBatchDecoder =
    fn(&[u8], &[Option<(usize, usize)>], usize, usize, &mut Vector, usize) -> Result<()>;

impl BinaryColumnDecoder {
    fn bind(ty: &LogicalType) -> Self {
        match ty {
            LogicalType::Boolean => Self::direct(ty, 1, decode_boolean_batch),
            LogicalType::TinyInt => Self::direct(ty, 2, decode_tinyint_batch),
            LogicalType::UTinyInt => Self::direct(ty, 2, decode_utinyint_batch),
            LogicalType::SmallInt => Self::direct(ty, 2, decode_smallint_batch),
            LogicalType::Integer => Self::direct(ty, 4, decode_integer_batch),
            LogicalType::USmallInt => Self::direct(ty, 4, decode_usmallint_batch),
            LogicalType::BigInt => Self::direct(ty, 8, decode_bigint_batch),
            LogicalType::UInteger => Self::direct(ty, 8, decode_uinteger_batch),
            LogicalType::Float => Self::direct(ty, 4, decode_float_batch),
            LogicalType::Double => Self::direct(ty, 8, decode_double_batch),
            LogicalType::Uuid => Self::direct(ty, 16, decode_uuid_batch),
            LogicalType::Date => Self::direct(ty, 4, decode_date_batch),
            LogicalType::Timestamp => Self::direct(ty, 8, decode_timestamp_batch),
            LogicalType::TimestampTz => Self::direct(ty, 8, decode_timestamptz_batch),
            LogicalType::Array(child, dimension) if matches!(**child, LogicalType::Float) => {
                Self::FloatArray {
                    dimension: *dimension,
                }
            }
            other => Self::generic(other, None),
        }
    }

    fn direct(
        expected_type: &LogicalType,
        expected_width: usize,
        decode_batch: BinaryBatchDecoder,
    ) -> Self {
        Self::Direct {
            expected_type: expected_type.clone(),
            expected_width,
            decode_batch,
        }
    }

    fn generic(ty: &LogicalType, expected_width: Option<usize>) -> Self {
        Self::Generic {
            ty: ty.clone(),
            expected_width,
        }
    }

    fn expected_fixed_width(&self) -> Option<usize> {
        match self {
            Self::Direct { expected_width, .. } => Some(*expected_width),
            Self::FloatArray { dimension } => dimension
                .checked_mul(8)
                .and_then(|payload| payload.checked_add(20)),
            Self::Generic { expected_width, .. } => *expected_width,
        }
    }

    fn validate_field_len(
        &self,
        len: usize,
        max_field_bytes: usize,
        row: usize,
        column: usize,
    ) -> Result<()> {
        if len > max_field_bytes {
            return Err(paro_error::configuration_limit_exceeded(format!(
                "binary COPY row {row} column {column} exceeds the {max_field_bytes}-byte statement field limit",
            )));
        }
        if let Some(expected) = self.expected_fixed_width() {
            if len != expected {
                return Err(paro_error::protocol_violation(format!(
                    "binary COPY row {row} column {column} expected {expected} bytes, got {len}",
                )));
            }
            return Ok(());
        }
        Ok(())
    }
}

impl TableFunctionBindData for ReadBinaryBindData {
    fn clone_box(&self) -> Box<dyn TableFunctionBindData> {
        Box::new(self.clone())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

struct BinaryCopyReader {
    input: Box<dyn Read + Send>,
    row_number: usize,
    finished: bool,
}

impl BinaryCopyReader {
    fn new(mut input: Box<dyn Read + Send>) -> Result<Self> {
        let mut header = [0_u8; BINARY_HEADER_LEN];
        input
            .read_exact(&mut header)
            .map_err(|_| paro_error::protocol_violation("binary COPY header is truncated"))?;
        if &header[..BINARY_SIGNATURE.len()] != BINARY_SIGNATURE {
            return Err(paro_error::protocol_violation(
                "COPY file signature not recognized",
            ));
        }
        let flags = u32::from_be_bytes(header[11..15].try_into().expect("header width"));
        if flags & OIDS_FLAG != 0 {
            return Err(paro_error::protocol_violation(
                "binary COPY WITH OIDS is not supported",
            ));
        }
        if flags >> 16 != 0 {
            return Err(paro_error::protocol_violation(format!(
                "binary COPY contains unrecognized critical flags 0x{flags:08x}",
            )));
        }
        let extension_len = i32::from_be_bytes(header[15..19].try_into().expect("header width"));
        let extension_len = usize::try_from(extension_len).map_err(|_| {
            paro_error::protocol_violation("binary COPY header extension length is negative")
        })?;
        let mut remaining = extension_len;
        let mut discard = [0_u8; 4096];
        while remaining > 0 {
            let count = remaining.min(discard.len());
            input.read_exact(&mut discard[..count]).map_err(|_| {
                paro_error::protocol_violation("binary COPY header extension is truncated")
            })?;
            remaining -= count;
        }
        Ok(Self {
            input,
            row_number: 0,
            finished: false,
        })
    }

    fn read_exact_into(&mut self, output: &mut [u8], field: &str) -> Result<()> {
        self.input.read_exact(output).map_err(|_| {
            paro_error::protocol_violation(format!("binary COPY {field} is truncated"))
        })
    }

    fn read_i16_or_eof(&mut self) -> Result<Option<i16>> {
        let mut bytes = [0_u8; 2];
        let first = loop {
            match self.input.read(&mut bytes[..1]) {
                Err(err) if err.kind() == std::io::ErrorKind::Interrupted => continue,
                result => break result.map_err(paro_error::io)?,
            }
        };
        if first == 0 {
            return Ok(None);
        }
        self.read_exact_into(&mut bytes[1..], "row field count")?;
        Ok(Some(i16::from_be_bytes(bytes)))
    }

    fn read_i32(&mut self, field: &str) -> Result<i32> {
        let mut bytes = [0_u8; 4];
        self.read_exact_into(&mut bytes, field)?;
        Ok(i32::from_be_bytes(bytes))
    }

    fn reject_trailing_data(&mut self) -> Result<()> {
        let mut byte = [0_u8; 1];
        let read = loop {
            match self.input.read(&mut byte) {
                Err(err) if err.kind() == std::io::ErrorKind::Interrupted => continue,
                result => break result.map_err(paro_error::io)?,
            }
        };
        if read != 0 {
            return Err(paro_error::protocol_violation(
                "binary COPY contains data after its EOF marker",
            ));
        }
        Ok(())
    }
}

struct ReadBinaryGlobalState {
    reader: Mutex<BinaryCopyReader>,
    max_field_bytes: usize,
    target_batch_bytes: usize,
}

impl GlobalTableFunctionState for ReadBinaryGlobalState {
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
struct ReadBinaryLocalState {
    batch_bytes: Vec<u8>,
    /// Flattened row-major `(start, end)` ranges. `None` represents SQL NULL.
    field_ranges: Vec<Option<(usize, usize)>>,
}

impl LocalTableFunctionState for ReadBinaryLocalState {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

pub fn bind_copy_from(
    source: CopyFromSource,
    names: &[String],
    types: &[LogicalType],
) -> Result<Box<dyn TableFunctionBindData>> {
    if names.len() != types.len() {
        return Err(paro_error::invalid_input(
            "COPY FROM input names/types length mismatch",
        ));
    }
    if let Some(unsupported) = types.iter().find(|ty| !is_binary_recv_supported(ty)) {
        return Err(paro_error::not_supported(format!(
            "COPY FROM BINARY does not support type {unsupported}",
        )));
    }
    Ok(Box::new(ReadBinaryBindData {
        source,
        types: types.to_vec(),
        decoders: types.iter().map(BinaryColumnDecoder::bind).collect(),
    }))
}

pub fn create_read_binary_function() -> TableFunction {
    TableFunction::new("read_binary", Vec::new())
        .with_init_global(read_binary_init_global)
        .with_init_local(read_binary_init_local)
        .with_function(read_binary_function)
}

fn read_binary_init_global(
    input: &TableFunctionInitInput,
) -> Result<Option<Box<dyn GlobalTableFunctionState>>> {
    let bind_data = input
        .bind_data
        .and_then(|data| data.as_any().downcast_ref::<ReadBinaryBindData>())
        .ok_or_else(|| paro_error::internal("invalid read_binary bind data"))?;
    let reader: Box<dyn Read + Send> = match &bind_data.source {
        CopyFromSource::Stdin => input.copy_stdin_source()?.take_reader()?,
        CopyFromSource::File(path) => Box::new(BufReader::new(std::fs::File::open(path).map_err(
            |err| paro_error::io_error(format!("open binary COPY file '{path}': {err}")),
        )?)),
    };
    let memory_limit = input
        .runtime
        .memory_limit_bytes()
        .filter(|limit| *limit > 0)
        .ok_or_else(|| paro_error::internal("binary COPY requires a statement memory limit"))?;
    let max_field_bytes = (memory_limit / 4).max(1);
    let target_batch_bytes = (memory_limit / 128)
        .max(MIN_BINARY_COPY_BATCH_BYTES)
        .min(max_field_bytes);
    Ok(Some(Box::new(ReadBinaryGlobalState {
        reader: Mutex::new(BinaryCopyReader::new(reader)?),
        max_field_bytes,
        target_batch_bytes,
    })))
}

fn read_binary_init_local(
    _input: &TableFunctionInitInput,
    _global_state: Option<&dyn GlobalTableFunctionState>,
) -> Result<Option<Box<dyn LocalTableFunctionState>>> {
    Ok(Some(Box::new(ReadBinaryLocalState::default())))
}

fn read_binary_function(
    input: &mut TableFunctionInput,
    output: &mut Chunk,
) -> Result<TableFunctionResult> {
    let bind_data = input
        .bind_data
        .and_then(|data| data.as_any().downcast_ref::<ReadBinaryBindData>())
        .ok_or_else(|| paro_error::internal("invalid read_binary bind data"))?;
    let global = input
        .global_state
        .and_then(|state| state.as_any().downcast_ref::<ReadBinaryGlobalState>())
        .ok_or_else(|| paro_error::internal("invalid read_binary global state"))?;
    let local = input
        .local_state
        .as_mut()
        .and_then(|state| state.as_any_mut().downcast_mut::<ReadBinaryLocalState>())
        .ok_or_else(|| paro_error::internal("invalid read_binary local state"))?;
    let mut reader = global
        .reader
        .lock()
        .map_err(|err| paro_error::internal(err.to_string()))?;
    if reader.finished {
        output.set_cardinality(0);
        return Ok(TableFunctionResult::Finished);
    }

    let capacity = output.capacity().min(VECTOR_SIZE);
    local.batch_bytes.clear();
    local.field_ranges.clear();
    local
        .field_ranges
        .try_reserve(capacity.saturating_mul(bind_data.decoders.len()))
        .map_err(|_| paro_error::out_of_memory("binary COPY field directory allocation failed"))?;
    let mut produced = 0;
    while produced < capacity {
        let Some(field_count) = reader.read_i16_or_eof()? else {
            return Err(paro_error::protocol_violation(
                "binary COPY stream is missing its EOF marker",
            ));
        };
        if field_count == -1 {
            reader.reject_trailing_data()?;
            reader.finished = true;
            break;
        }
        if field_count < 0 || field_count as usize != bind_data.types.len() {
            return Err(paro_error::protocol_violation(format!(
                "binary COPY row {} has {field_count} fields, expected {}",
                reader.row_number + 1,
                bind_data.types.len(),
            )));
        }

        for (column_idx, decoder) in bind_data.decoders.iter().enumerate() {
            let field_len = reader.read_i32("field length")?;
            if field_len == -1 {
                local.field_ranges.push(None);
                continue;
            }
            let field_len = usize::try_from(field_len).map_err(|_| {
                paro_error::protocol_violation(format!(
                    "binary COPY row {} column {} has invalid field length {field_len}",
                    reader.row_number + 1,
                    column_idx + 1,
                ))
            })?;
            decoder.validate_field_len(
                field_len,
                global.max_field_bytes,
                reader.row_number + 1,
                column_idx + 1,
            )?;
            let start = local.batch_bytes.len();
            let end = start.checked_add(field_len).ok_or_else(|| {
                paro_error::configuration_limit_exceeded("binary COPY batch byte size overflow")
            })?;
            local.batch_bytes.try_reserve(field_len).map_err(|_| {
                paro_error::out_of_memory("binary COPY field payload allocation failed")
            })?;
            local.batch_bytes.resize(end, 0);
            reader.read_exact_into(&mut local.batch_bytes[start..end], "field payload")?;
            local.field_ranges.push(Some((start, end)));
        }
        reader.row_number += 1;
        produced += 1;
        if local.batch_bytes.len() >= global.target_batch_bytes {
            break;
        }
    }

    decode_binary_batch(bind_data, local, output, produced)?;
    if local.batch_bytes.capacity() > global.target_batch_bytes.saturating_mul(2) {
        local.batch_bytes = Vec::new();
        local.field_ranges = Vec::new();
    }
    output.set_cardinality(produced);
    if reader.finished {
        Ok(TableFunctionResult::Finished)
    } else {
        Ok(TableFunctionResult::HaveMoreOutput)
    }
}

fn decode_binary_batch(
    bind_data: &ReadBinaryBindData,
    local: &ReadBinaryLocalState,
    output: &mut Chunk,
    row_count: usize,
) -> Result<()> {
    let column_count = bind_data.decoders.len();
    for (column_idx, decoder) in bind_data.decoders.iter().enumerate() {
        let column = output
            .column_mut(column_idx)
            .ok_or_else(|| paro_error::internal("binary COPY output column missing"))?;
        match decoder {
            BinaryColumnDecoder::Direct {
                expected_type,
                decode_batch,
                ..
            } => {
                if column.logical_type() != expected_type {
                    return Err(paro_error::internal(format!(
                        "binary COPY decoder for {expected_type} received {} storage",
                        column.logical_type()
                    )));
                }
                decode_batch(
                    &local.batch_bytes,
                    &local.field_ranges,
                    column_count,
                    column_idx,
                    column,
                    row_count,
                )?
            }
            BinaryColumnDecoder::FloatArray { dimension } => decode_float_array_batch(
                &local.batch_bytes,
                &local.field_ranges,
                column_count,
                column_idx,
                column,
                row_count,
                *dimension,
            )?,
            BinaryColumnDecoder::Generic { ty, .. } => decode_generic_batch(
                &local.batch_bytes,
                &local.field_ranges,
                column_count,
                column_idx,
                column,
                row_count,
                ty,
            )?,
        }
    }
    Ok(())
}

fn mark_binary_nulls(
    field_ranges: &[Option<(usize, usize)>],
    column_count: usize,
    column_idx: usize,
    column: &mut Vector,
    row_count: usize,
) {
    for row in 0..row_count {
        column.set_null(row, field_ranges[row * column_count + column_idx].is_none());
    }
}

fn for_each_binary_field(
    batch_bytes: &[u8],
    field_ranges: &[Option<(usize, usize)>],
    column_count: usize,
    column_idx: usize,
    row_count: usize,
    mut decode: impl FnMut(&[u8], usize) -> Result<()>,
) -> Result<()> {
    for row in 0..row_count {
        if let Some((start, end)) = field_ranges[row * column_count + column_idx] {
            decode(&batch_bytes[start..end], row)?;
        }
    }
    Ok(())
}

fn decode_direct_batch<T: Copy + 'static>(
    batch_bytes: &[u8],
    field_ranges: &[Option<(usize, usize)>],
    column_count: usize,
    column_idx: usize,
    column: &mut Vector,
    row_count: usize,
    mut decode: impl FnMut(&[u8]) -> Result<T>,
) -> Result<()> {
    mark_binary_nulls(field_ranges, column_count, column_idx, column, row_count);
    let values = column.as_mut_slice::<T>();
    for_each_binary_field(
        batch_bytes,
        field_ranges,
        column_count,
        column_idx,
        row_count,
        |bytes, row| {
            values[row] = decode(bytes)?;
            Ok(())
        },
    )
}

macro_rules! fixed_binary_batch_decoder {
    ($name:ident, $ty:ty, $decode:expr) => {
        fn $name(
            batch_bytes: &[u8],
            field_ranges: &[Option<(usize, usize)>],
            column_count: usize,
            column_idx: usize,
            column: &mut Vector,
            row_count: usize,
        ) -> Result<()> {
            decode_direct_batch(
                batch_bytes,
                field_ranges,
                column_count,
                column_idx,
                column,
                row_count,
                $decode,
            )
        }
    };
}

fixed_binary_batch_decoder!(decode_boolean_batch, bool, |bytes: &[u8]| Ok(bytes[0] != 0));
fixed_binary_batch_decoder!(decode_smallint_batch, i16, |bytes: &[u8]| Ok(
    i16::from_be_bytes(bytes.try_into().expect("validated width"))
));
fixed_binary_batch_decoder!(decode_integer_batch, i32, |bytes: &[u8]| Ok(
    i32::from_be_bytes(bytes.try_into().expect("validated width"))
));
fixed_binary_batch_decoder!(decode_bigint_batch, i64, |bytes: &[u8]| Ok(
    i64::from_be_bytes(bytes.try_into().expect("validated width"))
));
fixed_binary_batch_decoder!(decode_float_batch, f32, |bytes: &[u8]| Ok(
    f32::from_be_bytes(bytes.try_into().expect("validated width"))
));
fixed_binary_batch_decoder!(decode_double_batch, f64, |bytes: &[u8]| Ok(
    f64::from_be_bytes(bytes.try_into().expect("validated width"))
));
fixed_binary_batch_decoder!(decode_uuid_batch, u128, |bytes: &[u8]| Ok(
    u128::from_be_bytes(bytes.try_into().expect("validated width"))
));
fixed_binary_batch_decoder!(decode_date_batch, i32, |bytes: &[u8]| {
    pg_date_to_unix_days(i32::from_be_bytes(
        bytes.try_into().expect("validated width"),
    ))
});
fixed_binary_batch_decoder!(decode_timestamp_batch, i64, |bytes: &[u8]| {
    pg_timestamp_to_unix_micros(i64::from_be_bytes(
        bytes.try_into().expect("validated width"),
    ))
});
fixed_binary_batch_decoder!(decode_timestamptz_batch, i64, |bytes: &[u8]| {
    pg_timestamp_to_unix_micros(i64::from_be_bytes(
        bytes.try_into().expect("validated width"),
    ))
});
fixed_binary_batch_decoder!(decode_tinyint_batch, i8, |bytes: &[u8]| {
    let value = i16::from_be_bytes(bytes.try_into().expect("validated width"));
    i8::try_from(value).map_err(|_| paro_error::invalid_value("tinyint", value.to_string()))
});
fixed_binary_batch_decoder!(decode_utinyint_batch, u8, |bytes: &[u8]| {
    let value = i16::from_be_bytes(bytes.try_into().expect("validated width"));
    u8::try_from(value).map_err(|_| paro_error::invalid_value("utinyint", value.to_string()))
});
fixed_binary_batch_decoder!(decode_usmallint_batch, u16, |bytes: &[u8]| {
    let value = i32::from_be_bytes(bytes.try_into().expect("validated width"));
    u16::try_from(value).map_err(|_| paro_error::invalid_value("usmallint", value.to_string()))
});
fixed_binary_batch_decoder!(decode_uinteger_batch, u32, |bytes: &[u8]| {
    let value = i64::from_be_bytes(bytes.try_into().expect("validated width"));
    u32::try_from(value).map_err(|_| paro_error::invalid_value("uinteger", value.to_string()))
});

#[allow(clippy::too_many_arguments)]
fn decode_generic_batch(
    batch_bytes: &[u8],
    field_ranges: &[Option<(usize, usize)>],
    column_count: usize,
    column_idx: usize,
    column: &mut Vector,
    row_count: usize,
    ty: &LogicalType,
) -> Result<()> {
    mark_binary_nulls(field_ranges, column_count, column_idx, column, row_count);
    for_each_binary_field(
        batch_bytes,
        field_ranges,
        column_count,
        column_idx,
        row_count,
        |bytes, row| {
            column.set_value(row, &decode_binary_value(bytes, ty)?);
            Ok(())
        },
    )
}

fn decode_float_array_batch(
    batch_bytes: &[u8],
    field_ranges: &[Option<(usize, usize)>],
    column_count: usize,
    column_idx: usize,
    column: &mut paro_common::vector::Vector,
    row_count: usize,
    dimension: usize,
) -> Result<()> {
    mark_binary_nulls(field_ranges, column_count, column_idx, column, row_count);
    let child = ArrayVector::get_entry_mut(column);
    let child_values = child.as_mut_slice::<f32>();
    for row in 0..row_count {
        let Some((start, end)) = field_ranges[row * column_count + column_idx] else {
            continue;
        };
        let child_start = row
            .checked_mul(dimension)
            .ok_or_else(|| paro_error::protocol_violation("VECTOR child offset overflow"))?;
        let child_end = child_start
            .checked_add(dimension)
            .ok_or_else(|| paro_error::protocol_violation("VECTOR child offset overflow"))?;
        let target = child_values
            .get_mut(child_start..child_end)
            .ok_or_else(|| paro_error::internal("VECTOR output child capacity is too small"))?;
        decode_float_array_into_slice(&batch_bytes[start..end], target, dimension)?;
    }
    Ok(())
}

fn decode_float_array_into_slice(bytes: &[u8], target: &mut [f32], dimension: usize) -> Result<()> {
    let mut input = BinaryInput::new(bytes);
    let dimensions = input.read_i32("array dimension count")?;
    let has_null = input.read_i32("array null flag")?;
    let element_oid = input.read_u32("array element OID")?;
    let float_oid = LogicalType::Float.pg_descriptor().oid;
    if dimensions != 1 || element_oid != float_oid {
        return Err(paro_error::protocol_violation(format!(
            "VECTOR({dimension}) binary value must be a one-dimensional FLOAT4 array",
        )));
    }
    let length = input.read_i32("array length")?;
    let _lower_bound = input.read_i32("array lower bound")?;
    if length < 0 || length as usize != dimension {
        return Err(paro_error::invalid_value(
            format!("VECTOR({dimension})"),
            format!("binary array with {length} elements"),
        ));
    }
    if has_null != 0 {
        return Err(paro_error::invalid_value(
            format!("VECTOR({dimension})"),
            "binary array containing NULL elements",
        ));
    }

    for value in target {
        let element_len = input.read_i32("array element length")?;
        if element_len != 4 {
            return Err(paro_error::protocol_violation(format!(
                "VECTOR FLOAT4 element has {element_len} bytes, expected 4",
            )));
        }
        *value = f32::from_be_bytes(
            input
                .read_bytes(4, "FLOAT4 array element")?
                .try_into()
                .expect("checked width"),
        );
    }
    input.reject_trailing("VECTOR binary value")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn binary_payload() -> Vec<u8> {
        let mut data = BINARY_SIGNATURE.to_vec();
        data.extend_from_slice(&0_u32.to_be_bytes());
        data.extend_from_slice(&0_u32.to_be_bytes());
        data.extend_from_slice(&2_i16.to_be_bytes());
        data.extend_from_slice(&4_i32.to_be_bytes());
        data.extend_from_slice(&7_i32.to_be_bytes());

        let mut vector = Vec::new();
        vector.extend_from_slice(&1_i32.to_be_bytes());
        vector.extend_from_slice(&0_i32.to_be_bytes());
        vector.extend_from_slice(&LogicalType::Float.pg_descriptor().oid.to_be_bytes());
        vector.extend_from_slice(&3_i32.to_be_bytes());
        vector.extend_from_slice(&1_i32.to_be_bytes());
        for value in [1.0_f32, 2.0, 3.0] {
            vector.extend_from_slice(&4_i32.to_be_bytes());
            vector.extend_from_slice(&value.to_be_bytes());
        }
        data.extend_from_slice(&(vector.len() as i32).to_be_bytes());
        data.extend_from_slice(&vector);
        data.extend_from_slice(&(-1_i16).to_be_bytes());
        data
    }

    #[test]
    fn binary_copy_decodes_vector_directly_into_array_child() {
        let mut reader =
            BinaryCopyReader::new(Box::new(std::io::Cursor::new(binary_payload()))).unwrap();
        assert_eq!(reader.read_i16_or_eof().unwrap(), Some(2));
        assert_eq!(reader.read_i32("id length").unwrap(), 4);
        assert_eq!(reader.read_i32("id").unwrap(), 7);
        let vector_len = reader.read_i32("vector length").unwrap() as usize;
        let mut vector = vec![0_u8; vector_len];
        reader.read_exact_into(&mut vector, "vector").unwrap();
        let mut target = [0.0_f32; 3];
        decode_float_array_into_slice(&vector, &mut target, 3).unwrap();
        assert_eq!(target, [1.0, 2.0, 3.0]);
        assert_eq!(reader.read_i16_or_eof().unwrap(), Some(-1));
        reader.reject_trailing_data().unwrap();
    }

    #[test]
    fn binary_copy_rejects_unbounded_variable_field_before_allocation() {
        let decoder = BinaryColumnDecoder::generic(&LogicalType::Varchar, None);
        let max_field_bytes = 8 * 1024 * 1024;
        let error = decoder
            .validate_field_len(max_field_bytes + 1, max_field_bytes, 7, 3)
            .unwrap_err();
        assert!(error
            .to_string()
            .contains("exceeds the 8388608-byte statement field limit"));
    }
}
