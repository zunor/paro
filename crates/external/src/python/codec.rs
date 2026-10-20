// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! The only Paro/Arrow type conversion boundary. Native vectors remain native.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::*;
use arrow_buffer::{i256, NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use paro_common::allocator::Allocator;
use paro_common::chunk::Chunk;
use paro_common::error::{self as error, Result};
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use paro_common::vector::{Vector, VectorType};

use super::{MAX_BATCH_BYTES, MAX_BATCH_ROWS};

fn failure(message: impl std::fmt::Display) -> paro_common::error::ParoError {
    error::contract_violation(format!("Arrow type contract: {message}"))
}

pub fn data_type(ty: &LogicalType) -> Result<DataType> {
    use LogicalType as L;
    Ok(match ty {
        L::Boolean => DataType::Boolean,
        L::TinyInt => DataType::Int8,
        L::SmallInt => DataType::Int16,
        L::Integer => DataType::Int32,
        L::BigInt => DataType::Int64,
        L::UTinyInt => DataType::UInt8,
        L::USmallInt => DataType::UInt16,
        L::UInteger => DataType::UInt32,
        L::UBigInt => DataType::UInt64,
        L::HugeInt | L::UHugeInt => DataType::Decimal256(39, 0),
        L::Float => DataType::Float32,
        L::Double => DataType::Float64,
        L::Decimal { precision, scale } => DataType::Decimal128(*precision, *scale as i8),
        L::Varchar | L::VarcharCollation(_) | L::Json | L::Jsonb => DataType::Utf8,
        L::Blob => DataType::Binary,
        L::Uuid => DataType::FixedSizeBinary(16),
        L::Date => DataType::Date32,
        L::Time => DataType::Time64(TimeUnit::Microsecond),
        L::Timestamp => DataType::Timestamp(TimeUnit::Microsecond, None),
        L::TimestampTz => DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        L::Interval => DataType::Struct(
            vec![
                Arc::new(Field::new("months", DataType::Int32, true)),
                Arc::new(Field::new("days", DataType::Int32, true)),
                Arc::new(Field::new("micros", DataType::Int64, true)),
            ]
            .into(),
        ),
        L::List(child) => DataType::List(Arc::new(field("item", child)?)),
        L::Array(child, width) => DataType::FixedSizeList(
            Arc::new(field("item", child)?),
            i32::try_from(*width).map_err(failure)?,
        ),
        L::Struct(children) => DataType::Struct(
            children
                .iter()
                .map(|(name, ty)| field(name, ty).map(Arc::new))
                .collect::<Result<Vec<_>>>()?
                .into(),
        ),
        other => return Err(error::not_supported(format!("Python Arrow type {other:?}"))),
    })
}

pub fn field(name: &str, ty: &LogicalType) -> Result<Field> {
    Ok(
        Field::new(name, data_type(ty)?, true).with_metadata(HashMap::from([(
            "paro.logical_type".into(),
            serde_json::to_string(ty).map_err(failure)?,
        )])),
    )
}

pub fn schema(types: &[(String, LogicalType)]) -> Result<Arc<Schema>> {
    Ok(Arc::new(Schema::new(
        types
            .iter()
            .map(|(name, ty)| field(name, ty))
            .collect::<Result<Vec<_>>>()?,
    )))
}

fn logical_type(field: &Field) -> Result<LogicalType> {
    let ty: LogicalType = serde_json::from_str(
        field
            .metadata()
            .get("paro.logical_type")
            .ok_or_else(|| failure("missing Paro logical type metadata"))?,
    )
    .map_err(failure)?;
    if &data_type(&ty)? != field.data_type() {
        return Err(failure("logical type metadata does not match Arrow field"));
    }
    Ok(ty)
}

pub fn encode(chunk: &Chunk, schema: Arc<Schema>) -> Result<RecordBatch> {
    if chunk.size() > MAX_BATCH_ROWS {
        return Err(failure("input exceeds the Arrow row budget"));
    }
    if chunk.column_count() != schema.fields().len() {
        return Err(failure("input column count mismatch"));
    }
    // Bound value materialization before cloning variable-length or nested
    // cells. Dictionary/constant vectors can expand far beyond retained bytes.
    let mut remaining = MAX_BATCH_BYTES;
    for column in chunk.view().columns() {
        for row in 0..chunk.size() {
            charge_value(column, row, &mut remaining, 0)?;
        }
    }
    let arrays = chunk
        .view()
        .columns()
        .iter()
        .zip(schema.fields())
        .map(|(column, field)| {
            let ty = logical_type(field)?;
            if column.logical_type() != &ty {
                return Err(failure("input logical type does not match Arrow field"));
            }
            let values = (0..chunk.size())
                .map(|row| column.get_value(row))
                .collect::<Vec<_>>();
            array(&ty, &values)
        })
        .collect::<Result<Vec<_>>>()?;
    RecordBatch::try_new_with_options(
        schema,
        arrays,
        &RecordBatchOptions::new().with_row_count(Some(chunk.size())),
    )
    .map_err(failure)
}

fn charge_value(vector: &Vector, row: usize, remaining: &mut usize, depth: usize) -> Result<()> {
    if depth > 64 {
        return Err(failure("nested value exceeds Arrow depth budget"));
    }
    if vector.vector_type() == VectorType::Dictionary {
        return charge_value(
            vector
                .child()
                .ok_or_else(|| failure("dictionary has no child"))?,
            vector.selection().physical_index(row),
            remaining,
            depth + 1,
        );
    }
    let row = if vector.vector_type() == VectorType::Constant {
        0
    } else {
        row
    };
    if vector.is_null(row) {
        return charge_null(vector.logical_type(), remaining, depth);
    }
    let mut bytes = size_of::<Value>().saturating_add(type_clone_bytes(vector.logical_type(), 0));
    if !vector.is_null(row) {
        match vector.logical_type() {
            LogicalType::Varchar
            | LogicalType::VarcharCollation(_)
            | LogicalType::Json
            | LogicalType::Jsonb => {
                bytes = bytes.saturating_add(vector.get_string(row).map_or(0, str::len))
            }
            LogicalType::Blob => {
                bytes = bytes.saturating_add(vector.get_blob(row).map_or(0, <[u8]>::len))
            }
            LogicalType::List(_) | LogicalType::Array(_, _) => {
                let (start, count) = match vector.logical_type() {
                    LogicalType::Array(_, width) => (row * width, *width),
                    _ => {
                        // Native list entries are two u32 fields.
                        let [start, count]: [u32; 2] = unsafe { vector.get_flat(row) };
                        (start as usize, count as usize)
                    }
                };
                let child = vector
                    .child()
                    .ok_or_else(|| failure("collection has no child"))?;
                for index in start..start.saturating_add(count) {
                    charge_value(child, index, remaining, depth + 1)?;
                }
            }
            LogicalType::Struct(_) => {
                for child in vector
                    .children()
                    .ok_or_else(|| failure("struct has no fields"))?
                {
                    charge_value(child, row, remaining, depth + 1)?;
                }
            }
            _ => {}
        }
    }
    *remaining = remaining
        .checked_sub(bytes)
        .ok_or_else(|| failure("input exceeds Arrow materialization budget"))?;
    Ok(())
}

fn charge_null(ty: &LogicalType, remaining: &mut usize, depth: usize) -> Result<()> {
    if depth > 64 {
        return Err(failure("nested NULL exceeds Arrow depth budget"));
    }
    *remaining = remaining
        .checked_sub(size_of::<Value>().saturating_add(type_clone_bytes(ty, 0)))
        .ok_or_else(|| failure("input exceeds Arrow materialization budget"))?;
    // Fixed-size lists and structs still need physical child slots for NULLs.
    match ty {
        LogicalType::Array(child, width) => {
            for _ in 0..*width {
                charge_null(child, remaining, depth + 1)?;
            }
        }
        LogicalType::Struct(fields) => {
            for (_, child) in fields {
                charge_null(child, remaining, depth + 1)?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn type_clone_bytes(ty: &LogicalType, depth: usize) -> usize {
    if depth > 64 {
        return usize::MAX;
    }
    match ty {
        LogicalType::VarcharCollation(name) => name.len(),
        LogicalType::List(child) | LogicalType::Array(child, _) => {
            size_of::<LogicalType>().saturating_add(type_clone_bytes(child, depth + 1))
        }
        LogicalType::Struct(fields) => fields.iter().fold(0usize, |bytes, (name, ty)| {
            bytes
                .saturating_add(size_of::<(String, LogicalType)>())
                .saturating_add(name.len())
                .saturating_add(type_clone_bytes(ty, depth + 1))
        }),
        _ => 0,
    }
}

pub fn decode(
    batch: &RecordBatch,
    expected: &Schema,
    allocator: Arc<dyn Allocator>,
) -> Result<Vec<Vector>> {
    if batch.num_rows() > MAX_BATCH_ROWS || batch.get_array_memory_size() > MAX_BATCH_BYTES {
        return Err(failure("result exceeds Arrow batch budget"));
    }
    if batch.schema().as_ref() != expected {
        return Err(failure("result schema does not match bound routine"));
    }
    batch
        .columns()
        .iter()
        .zip(expected.fields())
        .map(|(array, field)| {
            let ty = logical_type(field)?;
            decode_array(array.as_ref(), &ty, allocator.clone())
        })
        .collect()
}

fn decode_array(
    array: &dyn Array,
    ty: &LogicalType,
    allocator: Arc<dyn Allocator>,
) -> Result<Vector> {
    // Native constructors reserve nested child capacity eagerly (including
    // List<Array<...>> with no elements). Bound that expansion before allocation.
    let mut remaining = MAX_BATCH_BYTES;
    charge_native_capacity(ty, array.len(), &mut remaining, 0)?;
    let mut vector = Vector::try_new(ty.clone(), array.len(), allocator.clone())?;
    vector.try_set_count(array.len())?;
    match ty {
        LogicalType::List(child) => {
            let list = array
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(|| failure("list mismatch"))?;
            let first = usize::try_from(list.value_offsets()[0]).map_err(failure)?;
            let last = usize::try_from(list.value_offsets()[array.len()]).map_err(failure)?;
            let values = list.values().slice(first, last - first);
            vector.set_child(Arc::new(decode_array(values.as_ref(), child, allocator)?));
            for row in 0..array.len() {
                let start =
                    u32::try_from(list.value_offsets()[row] as usize - first).map_err(failure)?;
                let len = u32::try_from(list.value_length(row)).map_err(failure)?;
                // The native list entry is two adjacent u32s, not an Arrow offset.
                unsafe {
                    vector.set_flat(row, [start, len]);
                }
            }
        }
        LogicalType::Array(child, width) => {
            let list = array
                .as_any()
                .downcast_ref::<FixedSizeListArray>()
                .ok_or_else(|| failure("array mismatch"))?;
            let values = list
                .values()
                .slice(list.offset() * width, array.len() * width);
            vector.set_child(Arc::new(decode_array(values.as_ref(), child, allocator)?));
        }
        LogicalType::Struct(fields) => {
            let structure = array
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| failure("struct mismatch"))?;
            let children = fields
                .iter()
                .enumerate()
                .map(|(index, (_, ty))| {
                    decode_array(structure.column(index).as_ref(), ty, allocator.clone())
                        .map(Arc::new)
                })
                .collect::<Result<Vec<_>>>()?;
            vector.set_children(children);
        }
        _ => {
            for row in 0..array.len() {
                if !vector.try_set_scalar_value(row, &value(array, ty, row)?)? {
                    return Err(failure("unexpected nested scalar"));
                }
            }
        }
    }
    for row in 0..array.len() {
        if array.is_null(row) {
            vector.try_set_null(row, true)?;
        }
    }
    Ok(vector)
}

fn charge_native_capacity(
    ty: &LogicalType,
    count: usize,
    remaining: &mut usize,
    depth: usize,
) -> Result<()> {
    if depth > 64 {
        return Err(failure("nested result exceeds Arrow depth budget"));
    }
    let bytes = count
        .saturating_mul(ty.type_size())
        .saturating_add(count.div_ceil(8))
        .saturating_add(type_clone_bytes(ty, 0));
    *remaining = remaining
        .checked_sub(bytes)
        .ok_or_else(|| failure("result exceeds native materialization budget"))?;
    match ty {
        LogicalType::List(child) => charge_native_capacity(child, count, remaining, depth + 1)?,
        LogicalType::Array(child, width) => {
            charge_native_capacity(child, count.saturating_mul(*width), remaining, depth + 1)?
        }
        LogicalType::Struct(fields) => {
            for (_, child) in fields {
                charge_native_capacity(child, count, remaining, depth + 1)?;
            }
        }
        _ => {}
    }
    Ok(())
}

pub fn array(ty: &LogicalType, values: &[Value]) -> Result<ArrayRef> {
    array_values(ty, &values.iter().collect::<Vec<_>>())
}

fn array_values(ty: &LogicalType, values: &[&Value]) -> Result<ArrayRef> {
    use LogicalType as L;
    macro_rules! primitive {
        ($array:ident, $variant:ident) => {
            Arc::new($array::from(
                values
                    .iter()
                    .map(|value| match value {
                        Value::Null(_) => Ok(None),
                        Value::$variant(value) => Ok(Some(*value)),
                        other => Err(failure(format!("expected {ty:?}, got {other:?}"))),
                    })
                    .collect::<Result<Vec<_>>>()?,
            )) as ArrayRef
        };
    }
    let nulls = Some(NullBuffer::from(
        values
            .iter()
            .map(|v| !matches!(v, Value::Null(_)))
            .collect::<Vec<_>>(),
    ));
    Ok(match ty {
        L::Boolean => primitive!(BooleanArray, Boolean),
        L::TinyInt => primitive!(Int8Array, TinyInt),
        L::SmallInt => primitive!(Int16Array, SmallInt),
        L::Integer => primitive!(Int32Array, Integer),
        L::BigInt => primitive!(Int64Array, BigInt),
        L::UTinyInt => primitive!(UInt8Array, UTinyInt),
        L::USmallInt => primitive!(UInt16Array, USmallInt),
        L::UInteger => primitive!(UInt32Array, UInteger),
        L::UBigInt => primitive!(UInt64Array, UBigInt),
        L::Float => primitive!(Float32Array, Float),
        L::Double => primitive!(Float64Array, Double),
        L::Date => primitive!(Date32Array, Date),
        L::Time => primitive!(Time64MicrosecondArray, Time),
        L::Timestamp => primitive!(TimestampMicrosecondArray, Timestamp),
        L::TimestampTz => {
            let array = primitive!(TimestampMicrosecondArray, TimestampTz);
            Arc::new(
                array
                    .as_any()
                    .downcast_ref::<TimestampMicrosecondArray>()
                    .unwrap()
                    .clone()
                    .with_timezone("UTC"),
            )
        }
        L::Decimal { precision, scale } => Arc::new(
            Decimal128Array::from(
                values
                    .iter()
                    .map(|v| match v {
                        Value::Null(_) => Ok(None),
                        Value::Decimal(v, p, s) if p == precision && s == scale => Ok(Some(*v)),
                        _ => Err(failure("decimal descriptor mismatch")),
                    })
                    .collect::<Result<Vec<_>>>()?,
            )
            .with_precision_and_scale(*precision, *scale as i8)
            .map_err(failure)?,
        ),
        L::HugeInt | L::UHugeInt => Arc::new(
            Decimal256Array::from(
                values
                    .iter()
                    .map(|v| match v {
                        Value::Null(_) => Ok(None),
                        Value::HugeInt(v) => Ok(Some(i256::from_i128(*v))),
                        Value::UHugeInt(v) => {
                            let mut bytes = [0; 32];
                            bytes[16..].copy_from_slice(&v.to_be_bytes());
                            Ok(Some(i256::from_be_bytes(bytes)))
                        }
                        _ => Err(failure("128-bit integer mismatch")),
                    })
                    .collect::<Result<Vec<_>>>()?,
            )
            .with_precision_and_scale(39, 0)
            .map_err(failure)?,
        ),
        L::Varchar | L::VarcharCollation(_) | L::Json | L::Jsonb => Arc::new(StringArray::from(
            values
                .iter()
                .map(|v| match v {
                    Value::Null(_) => Ok(None),
                    Value::Varchar(v) => Ok(Some(v.as_str())),
                    _ => Err(failure("string mismatch")),
                })
                .collect::<Result<Vec<_>>>()?,
        )),
        L::Blob => Arc::new(BinaryArray::from(
            values
                .iter()
                .map(|v| match v {
                    Value::Null(_) => Ok(None),
                    Value::Blob(v) => Ok(Some(v.as_slice())),
                    _ => Err(failure("binary mismatch")),
                })
                .collect::<Result<Vec<_>>>()?,
        )),
        L::Uuid => {
            let mut builder = builder::FixedSizeBinaryBuilder::with_capacity(values.len(), 16);
            for value in values {
                match value {
                    Value::Null(_) => builder.append_null(),
                    Value::Uuid(value) => {
                        builder.append_value(value.to_be_bytes()).map_err(failure)?
                    }
                    _ => return Err(failure("UUID mismatch")),
                }
            }
            Arc::new(builder.finish())
        }
        L::Interval => {
            let fields = match data_type(ty)? {
                DataType::Struct(fields) => fields,
                _ => unreachable!(),
            };
            let mut months = Vec::new();
            let mut days = Vec::new();
            let mut micros = Vec::new();
            for value in values {
                let (m, d, u) = match value {
                    Value::Interval(m, d, u) => (Some(*m), Some(*d), Some(*u)),
                    Value::Null(_) => (None, None, None),
                    _ => return Err(failure("interval mismatch")),
                };
                months.push(m);
                days.push(d);
                micros.push(u);
            }
            Arc::new(
                StructArray::try_new(
                    fields,
                    vec![
                        Arc::new(Int32Array::from(months)),
                        Arc::new(Int32Array::from(days)),
                        Arc::new(Int64Array::from(micros)),
                    ],
                    nulls,
                )
                .map_err(failure)?,
            )
        }
        L::List(child) | L::Array(child, _) => {
            let mut children = Vec::new();
            let null_value = Value::Null(*child.clone());
            let mut offsets = vec![0i32];
            for value in values {
                match value {
                    Value::List(items, _) | Value::Array(items, _, _) => {
                        children.extend(items.iter())
                    }
                    Value::Null(_) => {
                        if let L::Array(_, width) = ty {
                            children.extend((0..*width).map(|_| &null_value));
                        }
                    }
                    _ => return Err(failure("nested array mismatch")),
                }
                offsets.push(i32::try_from(children.len()).map_err(failure)?);
            }
            let child_array = array_values(child, &children)?;
            let field = Arc::new(field("item", child)?);
            match ty {
                L::List(_) => Arc::new(
                    ListArray::try_new(
                        field,
                        OffsetBuffer::new(ScalarBuffer::from(offsets)),
                        child_array,
                        nulls,
                    )
                    .map_err(failure)?,
                ),
                L::Array(_, width) => Arc::new(
                    FixedSizeListArray::try_new(field, *width as i32, child_array, nulls)
                        .map_err(failure)?,
                ),
                _ => unreachable!(),
            }
        }
        L::Struct(fields) => {
            let arrays = fields
                .iter()
                .enumerate()
                .map(|(index, (_, ty))| {
                    let null_value = Value::Null(ty.clone());
                    let children = values
                        .iter()
                        .map(|value| match value {
                            Value::Null(_) => Ok(&null_value),
                            Value::Struct(items, _) => items
                                .get(index)
                                .ok_or_else(|| failure("struct field count")),
                            _ => Err(failure("struct mismatch")),
                        })
                        .collect::<Result<Vec<_>>>()?;
                    array_values(ty, &children)
                })
                .collect::<Result<Vec<_>>>()?;
            let fields = fields
                .iter()
                .map(|(name, ty)| field(name, ty).map(Arc::new))
                .collect::<Result<Vec<_>>>()?;
            Arc::new(StructArray::try_new(fields.into(), arrays, nulls).map_err(failure)?)
        }
        other => return Err(error::not_supported(format!("Python Arrow type {other:?}"))),
    })
}

pub fn value(array: &dyn Array, ty: &LogicalType, row: usize) -> Result<Value> {
    use LogicalType as L;
    if array.is_null(row) {
        return Ok(Value::Null(ty.clone()));
    }
    macro_rules! get {
        ($array:ident) => {
            array
                .as_any()
                .downcast_ref::<$array>()
                .ok_or_else(|| failure("physical type mismatch"))?
                .value(row)
        };
    }
    Ok(match ty {
        L::Boolean => Value::Boolean(get!(BooleanArray)),
        L::TinyInt => Value::TinyInt(get!(Int8Array)),
        L::SmallInt => Value::SmallInt(get!(Int16Array)),
        L::Integer => Value::Integer(get!(Int32Array)),
        L::BigInt => Value::BigInt(get!(Int64Array)),
        L::UTinyInt => Value::UTinyInt(get!(UInt8Array)),
        L::USmallInt => Value::USmallInt(get!(UInt16Array)),
        L::UInteger => Value::UInteger(get!(UInt32Array)),
        L::UBigInt => Value::UBigInt(get!(UInt64Array)),
        L::Float => Value::Float(get!(Float32Array)),
        L::Double => Value::Double(get!(Float64Array)),
        L::Date => Value::Date(get!(Date32Array)),
        L::Timestamp => Value::Timestamp(get!(TimestampMicrosecondArray)),
        L::TimestampTz => Value::TimestampTz(get!(TimestampMicrosecondArray)),
        L::Time => {
            let micros = get!(Time64MicrosecondArray);
            if !(0..86_400_000_000).contains(&micros) {
                return Err(failure("TIME outside a day"));
            }
            Value::Time(micros)
        }
        L::Decimal { precision, scale } => {
            let raw = get!(Decimal128Array);
            if raw.unsigned_abs() >= 10u128.pow(u32::from(*precision)) {
                return Err(failure("decimal precision overflow"));
            }
            Value::Decimal(raw, *precision, *scale)
        }
        L::HugeInt => Value::HugeInt(
            get!(Decimal256Array)
                .to_i128()
                .ok_or_else(|| failure("HUGEINT overflow"))?,
        ),
        L::UHugeInt => {
            let bytes = get!(Decimal256Array).to_be_bytes();
            if bytes[..16].iter().any(|byte| *byte != 0) {
                return Err(failure("UHUGEINT overflow"));
            }
            Value::UHugeInt(u128::from_be_bytes(bytes[16..].try_into().unwrap()))
        }
        L::Varchar | L::VarcharCollation(_) | L::Json | L::Jsonb => {
            let text = get!(StringArray);
            if matches!(ty, L::Json | L::Jsonb) {
                serde_json::from_str::<serde_json::Value>(text).map_err(failure)?;
            }
            Value::Varchar(text.into())
        }
        L::Blob => Value::Blob(get!(BinaryArray).to_vec()),
        L::Uuid => Value::Uuid(u128::from_be_bytes(
            get!(FixedSizeBinaryArray).try_into().map_err(failure)?,
        )),
        L::List(child) | L::Array(child, _) => {
            let items = if matches!(ty, L::List(_)) {
                get!(ListArray)
            } else {
                get!(FixedSizeListArray)
            };
            let values = (0..items.len())
                .map(|row| value(items.as_ref(), child, row))
                .collect::<Result<Vec<_>>>()?;
            match ty {
                L::Array(_, width) => Value::Array(values, *child.clone(), *width),
                _ => Value::List(values, *child.clone()),
            }
        }
        L::Struct(fields) => {
            let structure = array
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| failure("struct mismatch"))?;
            Value::Struct(
                fields
                    .iter()
                    .enumerate()
                    .map(|(index, (_, ty))| value(structure.column(index).as_ref(), ty, row))
                    .collect::<Result<Vec<_>>>()?,
                fields.clone(),
            )
        }
        L::Interval => {
            let structure = array
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| failure("interval mismatch"))?;
            let m = value(structure.column(0).as_ref(), &L::Integer, row)?;
            let d = value(structure.column(1).as_ref(), &L::Integer, row)?;
            let u = value(structure.column(2).as_ref(), &L::BigInt, row)?;
            match (m, d, u) {
                (Value::Integer(m), Value::Integer(d), Value::BigInt(u)) => {
                    Value::Interval(m, d, u)
                }
                _ => return Err(failure("null interval component")),
            }
        }
        other => return Err(error::not_supported(format!("Python Arrow type {other:?}"))),
    })
}
