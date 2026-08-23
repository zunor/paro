// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! PostgreSQL binary COPY reader.

use std::any::Any;
use std::io::{BufReader, Read};
use std::sync::Mutex;

use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::types::LogicalType;
use paro_common::vector::{ArrayVector, VECTOR_SIZE};

use crate::copy::CopyFromSource;
use crate::pg_binary::{decode_binary_value, is_binary_recv_supported, BinaryInput};

use super::{
    GlobalTableFunctionState, LocalTableFunctionState, TableFunction, TableFunctionBindData,
    TableFunctionInitInput, TableFunctionInput, TableFunctionResult,
};

const BINARY_SIGNATURE: &[u8; 11] = b"PGCOPY\n\xff\r\n\0";
const BINARY_HEADER_LEN: usize = 19;
const OIDS_FLAG: u32 = 1 << 16;

#[derive(Debug, Clone)]
struct ReadBinaryBindData {
    source: CopyFromSource,
    types: Vec<LogicalType>,
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
        let first = self.input.read(&mut bytes[..1]).map_err(paro_error::io)?;
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
        if self.input.read(&mut byte).map_err(paro_error::io)? != 0 {
            return Err(paro_error::protocol_violation(
                "binary COPY contains data after its EOF marker",
            ));
        }
        Ok(())
    }
}

struct ReadBinaryGlobalState {
    reader: Mutex<BinaryCopyReader>,
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
    field_buffer: Vec<u8>,
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
        CopyFromSource::Stdin => input.copy_stdin_source()?.open_reader(),
        CopyFromSource::File(path) => Box::new(BufReader::new(std::fs::File::open(path).map_err(
            |err| paro_error::io_error(format!("open binary COPY file '{path}': {err}")),
        )?)),
    };
    Ok(Some(Box::new(ReadBinaryGlobalState {
        reader: Mutex::new(BinaryCopyReader::new(reader)?),
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

        for (column_idx, ty) in bind_data.types.iter().enumerate() {
            let field_len = reader.read_i32("field length")?;
            let column = output
                .column_mut(column_idx)
                .ok_or_else(|| paro_error::internal("binary COPY output column missing"))?;
            if field_len == -1 {
                column.set_null(produced, true);
                continue;
            }
            let field_len = usize::try_from(field_len).map_err(|_| {
                paro_error::protocol_violation(format!(
                    "binary COPY row {} column {} has invalid field length {field_len}",
                    reader.row_number + 1,
                    column_idx + 1,
                ))
            })?;
            local.field_buffer.resize(field_len, 0);
            reader.read_exact_into(&mut local.field_buffer, "field payload")?;
            decode_field_into_column(&local.field_buffer, ty, column, produced)?;
        }
        reader.row_number += 1;
        produced += 1;
    }

    output.set_cardinality(produced);
    if reader.finished {
        Ok(TableFunctionResult::Finished)
    } else {
        Ok(TableFunctionResult::HaveMoreOutput)
    }
}

fn decode_field_into_column(
    bytes: &[u8],
    ty: &LogicalType,
    column: &mut paro_common::vector::Vector,
    row: usize,
) -> Result<()> {
    if let LogicalType::Array(child, dimension) = ty {
        if matches!(child.as_ref(), LogicalType::Float) {
            return decode_float_array_into_column(bytes, column, row, *dimension);
        }
    }
    let value = decode_binary_value(bytes, ty)?;
    column.set_value(row, &value);
    Ok(())
}

fn decode_float_array_into_column(
    bytes: &[u8],
    column: &mut paro_common::vector::Vector,
    row: usize,
    dimension: usize,
) -> Result<()> {
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

    column.set_null(row, false);
    let child = ArrayVector::get_entry_mut(column);
    let start = row
        .checked_mul(dimension)
        .ok_or_else(|| paro_error::protocol_violation("VECTOR child offset overflow"))?;
    let end = start
        .checked_add(dimension)
        .ok_or_else(|| paro_error::protocol_violation("VECTOR child offset overflow"))?;
    let target = child
        .as_mut_slice::<f32>()
        .get_mut(start..end)
        .ok_or_else(|| paro_error::internal("VECTOR output child capacity is too small"))?;
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
        let allocator = std::sync::Arc::new(paro_common::allocator::default_allocator());
        let mut column = paro_common::vector::Vector::try_new_array(
            LogicalType::Array(Box::new(LogicalType::Float), 3),
            1,
            allocator,
        )
        .unwrap();
        column.set_count(1);
        decode_float_array_into_column(&vector, &mut column, 0, 3).unwrap();
        assert_eq!(
            ArrayVector::get_entry(&column).as_slice::<f32>()[..3],
            [1.0, 2.0, 3.0]
        );
        assert_eq!(reader.read_i16_or_eof().unwrap(), Some(-1));
        reader.reject_trailing_data().unwrap();
    }
}
