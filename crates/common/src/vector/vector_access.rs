// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::{StringHeap, Vector, VectorBuffer, VectorType};
use crate::error::{self as paro_error, Result};
use crate::runtime_value::Value;
use crate::types::{LogicalType, StringView};
use std::sync::Arc;

impl Vector {
    /// Get value at index as a Value object.
    pub fn get_value(&self, idx: usize) -> Value {
        if self.is_null(idx) {
            return Value::Null(self.logical_type.clone());
        }

        match &self.logical_type {
            LogicalType::Boolean => Value::Boolean(self.get_bool(idx).unwrap()),
            LogicalType::TinyInt => Value::TinyInt(self.get_i8(idx).unwrap()),
            LogicalType::SmallInt => Value::SmallInt(self.get_i16(idx).unwrap()),
            LogicalType::Integer => Value::Integer(self.get_i32(idx).unwrap()),
            LogicalType::BigInt => Value::BigInt(self.get_i64(idx).unwrap()),
            LogicalType::HugeInt => Value::HugeInt(self.get_i128(idx).unwrap()),
            LogicalType::UTinyInt => Value::UTinyInt(self.get_u8(idx).unwrap()),
            LogicalType::USmallInt => Value::USmallInt(self.get_u16(idx).unwrap()),
            LogicalType::UInteger => Value::UInteger(self.get_u32(idx).unwrap()),
            LogicalType::UBigInt => Value::UBigInt(self.get_u64(idx).unwrap()),
            LogicalType::UHugeInt => Value::UHugeInt(self.get_u128(idx).unwrap()),
            LogicalType::Uuid => Value::Uuid(self.get_u128(idx).unwrap()),
            LogicalType::Float => Value::Float(self.get_f32(idx).unwrap()),
            LogicalType::Double => Value::Double(self.get_f64(idx).unwrap()),
            LogicalType::Decimal { precision, scale } => {
                let value = if *precision <= 18 {
                    self.get_i64(idx).unwrap() as i128
                } else {
                    self.get_i128(idx).unwrap()
                };
                Value::Decimal(value, *precision, *scale)
            }
            LogicalType::Varchar
            | LogicalType::VarcharCollation(_)
            | LogicalType::TsVector
            | LogicalType::TsQuery
            | LogicalType::Json
            | LogicalType::Jsonb
            | LogicalType::StringLiteral => {
                Value::Varchar(self.get_string(idx).unwrap().to_string())
            }
            LogicalType::Blob => Value::Blob(self.get_blob(idx).unwrap().to_vec()),
            LogicalType::Array(child_type, array_size) => {
                fn resolve_array_row(vector: &Vector, idx: usize) -> (&Vector, usize) {
                    match vector.vector_type() {
                        VectorType::Flat => (vector, idx),
                        VectorType::Constant => (vector, 0),
                        VectorType::Dictionary => {
                            let child = vector.child().expect("Dictionary vector missing child");
                            resolve_array_row(child, vector.selection().physical_index(idx))
                        }
                        VectorType::Sequence => {
                            panic!("Sequence vectors cannot be Array type");
                        }
                    }
                }

                let (base_vec, physical_idx) = resolve_array_row(self, idx);
                let child = base_vec.child.as_ref().expect("Array vector missing child");
                let offset = physical_idx * array_size;
                let mut children = Vec::with_capacity(*array_size);
                for i in 0..*array_size {
                    children.push(child.get_value(offset + i));
                }
                Value::Array(children, child_type.as_ref().clone(), *array_size)
            }
            LogicalType::List(child_type) => {
                // Resolve base vector + physical row for dictionary/constant
                fn resolve_collection_row(vector: &Vector, idx: usize) -> (&Vector, usize) {
                    match vector.vector_type() {
                        VectorType::Flat => (vector, idx),
                        VectorType::Constant => (vector, 0),
                        VectorType::Dictionary => {
                            let child = vector.child().expect("Dictionary vector missing child");
                            resolve_collection_row(child, vector.selection().physical_index(idx))
                        }
                        VectorType::Sequence => {
                            panic!("Sequence vectors cannot be List type");
                        }
                    }
                }

                let (base_vec, physical_idx) = resolve_collection_row(self, idx);
                let entry_base = base_vec.buffer.data();
                let entry_ptr = unsafe { entry_base.add(physical_idx * 8) as *const u32 };
                let offset = unsafe { std::ptr::read_unaligned(entry_ptr) as usize };
                let length = unsafe { std::ptr::read_unaligned(entry_ptr.add(1)) as usize };
                let child = base_vec.child.as_ref().expect("List vector missing child");
                let mut children = Vec::with_capacity(length);
                for i in 0..length {
                    children.push(child.get_value(offset + i));
                }
                Value::List(children, child_type.as_ref().clone())
            }
            LogicalType::Struct(fields) => {
                fn resolve_struct_row(vector: &Vector, idx: usize) -> (&Vector, usize) {
                    match vector.vector_type() {
                        VectorType::Flat => (vector, idx),
                        VectorType::Constant => (vector, 0),
                        VectorType::Dictionary => {
                            let child = vector.child().expect("Dictionary vector missing child");
                            resolve_struct_row(child, vector.selection().physical_index(idx))
                        }
                        VectorType::Sequence => {
                            panic!("Sequence vectors cannot be Struct type");
                        }
                    }
                }

                let (base_vec, physical_idx) = resolve_struct_row(self, idx);
                let children = base_vec.children().expect("Struct vector missing children");
                if children.len() != fields.len() {
                    panic!(
                        "Struct child count mismatch: expected {}, got {}",
                        fields.len(),
                        children.len()
                    );
                }

                let mut values = Vec::with_capacity(fields.len());
                for (child, _field) in children.iter().zip(fields.iter()) {
                    values.push(child.get_value(physical_idx));
                }
                Value::Struct(values, fields.clone())
            }
            LogicalType::Date => Value::Date(self.get_i32(idx).unwrap()),
            LogicalType::Timestamp => Value::Timestamp(self.get_i64(idx).unwrap()),
            LogicalType::TimestampTz => Value::TimestampTz(self.get_i64(idx).unwrap()),
            LogicalType::Time => Value::Time(self.get_i64(idx).unwrap()),
            LogicalType::Interval => {
                let (months, days, micros) = self.get_interval(idx).unwrap();
                Value::Interval(months, days, micros)
            }
            _ => Value::Null(self.logical_type.clone()),
        }
    }

    /// Get i64 value at index. Returns None if null.
    pub fn get_i64(&self, idx: usize) -> Option<i64> {
        if self.is_null(idx) {
            return None;
        }
        match self.vector_type {
            VectorType::Flat => {
                // SAFETY: We checked the vector type
                Some(unsafe { self.get_flat::<i64>(idx) })
            }
            VectorType::Constant => {
                // SAFETY: Constant stores value at index 0
                unsafe {
                    let ptr = self.buffer.data() as *const i64;
                    Some(*ptr)
                }
            }
            VectorType::Sequence => {
                // SAFETY: Sequence stores [start, increment]
                unsafe {
                    let ptr = self.buffer.data() as *const i64;
                    let start = *ptr;
                    let increment = *ptr.add(1);
                    Some(start + (idx as i64) * increment)
                }
            }
            VectorType::Dictionary => {
                let physical_idx = self.physical_index(idx);
                self.child.as_ref().unwrap().get_i64(physical_idx)
            }
        }
    }

    /// Get i8 value at index. Returns None if null.
    pub fn get_i8(&self, idx: usize) -> Option<i8> {
        if self.is_null(idx) {
            return None;
        }
        match self.vector_type {
            VectorType::Flat => Some(unsafe { self.get_flat::<i8>(idx) }),
            VectorType::Constant => Some(unsafe { self.get_flat::<i8>(0) }),
            VectorType::Dictionary => {
                let physical_idx = self.physical_index(idx);
                self.child.as_ref().unwrap().get_i8(physical_idx)
            }
            _ => None,
        }
    }

    /// Get i16 value at index. Returns None if null.
    pub fn get_i16(&self, idx: usize) -> Option<i16> {
        if self.is_null(idx) {
            return None;
        }
        match self.vector_type {
            VectorType::Flat => Some(unsafe { self.get_flat::<i16>(idx) }),
            VectorType::Constant => Some(unsafe { self.get_flat::<i16>(0) }),
            VectorType::Dictionary => {
                let physical_idx = self.physical_index(idx);
                self.child.as_ref().unwrap().get_i16(physical_idx)
            }
            _ => None,
        }
    }

    /// Get i32 value at index. Returns None if null.
    pub fn get_i32(&self, idx: usize) -> Option<i32> {
        if self.is_null(idx) {
            return None;
        }
        match self.vector_type {
            VectorType::Flat => Some(unsafe { self.get_flat::<i32>(idx) }),
            VectorType::Constant => Some(unsafe { self.get_flat::<i32>(0) }),
            VectorType::Dictionary => {
                let physical_idx = self.physical_index(idx);
                self.child.as_ref().unwrap().get_i32(physical_idx)
            }
            _ => None,
        }
    }

    /// Get i128 value at index. Returns None if null.
    pub fn get_i128(&self, idx: usize) -> Option<i128> {
        if self.is_null(idx) {
            return None;
        }
        match self.vector_type {
            VectorType::Flat => Some(unsafe { self.get_flat::<i128>(idx) }),
            VectorType::Constant => Some(unsafe { self.get_flat::<i128>(0) }),
            VectorType::Dictionary => {
                let physical_idx = self.physical_index(idx);
                self.child.as_ref().unwrap().get_i128(physical_idx)
            }
            _ => None,
        }
    }

    /// Get u8 value at index. Returns None if null.
    pub fn get_u8(&self, idx: usize) -> Option<u8> {
        if self.is_null(idx) {
            return None;
        }
        match self.vector_type {
            VectorType::Flat => Some(unsafe { self.get_flat::<u8>(idx) }),
            VectorType::Constant => Some(unsafe { self.get_flat::<u8>(0) }),
            VectorType::Dictionary => {
                let physical_idx = self.physical_index(idx);
                self.child.as_ref().unwrap().get_u8(physical_idx)
            }
            _ => None,
        }
    }

    /// Get u16 value at index. Returns None if null.
    pub fn get_u16(&self, idx: usize) -> Option<u16> {
        if self.is_null(idx) {
            return None;
        }
        match self.vector_type {
            VectorType::Flat => Some(unsafe { self.get_flat::<u16>(idx) }),
            VectorType::Constant => Some(unsafe { self.get_flat::<u16>(0) }),
            VectorType::Dictionary => {
                let physical_idx = self.physical_index(idx);
                self.child.as_ref().unwrap().get_u16(physical_idx)
            }
            _ => None,
        }
    }

    /// Get u32 value at index. Returns None if null.
    pub fn get_u32(&self, idx: usize) -> Option<u32> {
        if self.is_null(idx) {
            return None;
        }
        match self.vector_type {
            VectorType::Flat => Some(unsafe { self.get_flat::<u32>(idx) }),
            VectorType::Constant => Some(unsafe { self.get_flat::<u32>(0) }),
            VectorType::Dictionary => {
                let physical_idx = self.physical_index(idx);
                self.child.as_ref().unwrap().get_u32(physical_idx)
            }
            _ => None,
        }
    }

    /// Get u64 value at index. Returns None if null.
    pub fn get_u64(&self, idx: usize) -> Option<u64> {
        if self.is_null(idx) {
            return None;
        }
        match self.vector_type {
            VectorType::Flat => Some(unsafe { self.get_flat::<u64>(idx) }),
            VectorType::Constant => Some(unsafe { self.get_flat::<u64>(0) }),
            VectorType::Dictionary => {
                let physical_idx = self.physical_index(idx);
                self.child.as_ref().unwrap().get_u64(physical_idx)
            }
            _ => None,
        }
    }

    /// Get u128 value at index. Returns None if null.
    pub fn get_u128(&self, idx: usize) -> Option<u128> {
        if self.is_null(idx) {
            return None;
        }
        match self.vector_type {
            VectorType::Flat => Some(unsafe { self.get_flat::<u128>(idx) }),
            VectorType::Constant => Some(unsafe { self.get_flat::<u128>(0) }),
            VectorType::Dictionary => {
                let physical_idx = self.physical_index(idx);
                self.child.as_ref().unwrap().get_u128(physical_idx)
            }
            _ => None,
        }
    }

    /// Get f32 value at index. Returns None if null.
    pub fn get_f32(&self, idx: usize) -> Option<f32> {
        if self.is_null(idx) {
            return None;
        }
        match self.vector_type {
            VectorType::Flat => Some(unsafe { self.get_flat::<f32>(idx) }),
            VectorType::Constant => Some(unsafe { self.get_flat::<f32>(0) }),
            VectorType::Dictionary => {
                let physical_idx = self.physical_index(idx);
                self.child.as_ref().unwrap().get_f32(physical_idx)
            }
            _ => None,
        }
    }

    /// Get f64 value at index. Returns None if null.
    pub fn get_f64(&self, idx: usize) -> Option<f64> {
        if self.is_null(idx) {
            return None;
        }
        match self.vector_type {
            VectorType::Flat => Some(unsafe { self.get_flat::<f64>(idx) }),
            VectorType::Constant => Some(unsafe { self.get_flat::<f64>(0) }),
            VectorType::Dictionary => {
                let physical_idx = self.physical_index(idx);
                self.child.as_ref().unwrap().get_f64(physical_idx)
            }
            _ => None,
        }
    }

    /// Get string value at index. Returns None if null.
    pub fn get_string(&self, idx: usize) -> Option<&str> {
        if !self.logical_type.is_utf8_varlen() {
            return None;
        }
        if self.is_null(idx) {
            return None;
        }
        match self.vector_type {
            VectorType::Flat | VectorType::Constant => {
                let entry_idx = if self.vector_type == VectorType::Constant {
                    0
                } else {
                    idx
                };
                // SAFETY: We know buffer contains StringView array
                let inline_str = unsafe {
                    let entries = self.buffer.data() as *const StringView;
                    &*entries.add(entry_idx)
                };
                // SAFETY: textual vector write paths validate or accept only
                // UTF-8, and unsafe decoders must uphold the same invariant.
                Some(unsafe { inline_str.as_str_unchecked() })
            }
            VectorType::Dictionary => {
                let physical_idx = self.selection().physical_index(idx);
                self.child.as_ref()?.get_string(physical_idx)
            }
            _ => None,
        }
    }

    /// Get blob value at index. Returns None if null.
    pub fn get_blob(&self, idx: usize) -> Option<&[u8]> {
        if self.is_null(idx) {
            return None;
        }
        match self.vector_type {
            VectorType::Flat | VectorType::Constant => {
                let entry_idx = if self.vector_type == VectorType::Constant {
                    0
                } else {
                    idx
                };
                // SAFETY: We know buffer contains StringView array
                let inline_str = unsafe {
                    let entries = self.buffer.data() as *const StringView;
                    &*entries.add(entry_idx)
                };
                Some(inline_str.as_bytes())
            }
            VectorType::Dictionary => {
                let physical_idx = self.selection().physical_index(idx);
                self.child.as_ref()?.get_blob(physical_idx)
            }
            _ => None,
        }
    }

    /// Get bool value at index. Returns None if null.
    pub fn get_bool(&self, idx: usize) -> Option<bool> {
        if self.is_null(idx) {
            return None;
        }
        match self.vector_type {
            VectorType::Flat => Some(unsafe { self.get_flat::<bool>(idx) }),
            VectorType::Constant => unsafe {
                let ptr = self.buffer.data() as *const bool;
                Some(*ptr)
            },
            VectorType::Dictionary => {
                let physical_idx = self.physical_index(idx);
                self.child.as_ref().unwrap().get_bool(physical_idx)
            }
            _ => None,
        }
    }

    /// Get interval value at index. Returns `None` if null.
    ///
    /// Interval is stored as 16 bytes: months (i32) + days (i32) + micros (i64).
    pub fn get_interval(&self, idx: usize) -> Option<(i32, i32, i64)> {
        if self.is_null(idx) {
            return None;
        }
        match self.vector_type {
            VectorType::Flat => {
                // Interval is stored as: months (i32), days (i32), micros (i64)
                // Total: 4 + 4 + 8 = 16 bytes
                unsafe {
                    let base = self.buffer.data().add(idx * 16);
                    let months = *(base as *const i32);
                    let days = *(base.add(4) as *const i32);
                    let micros = *(base.add(8) as *const i64);
                    Some((months, days, micros))
                }
            }
            VectorType::Constant => unsafe {
                let base = self.buffer.data();
                let months = *(base as *const i32);
                let days = *(base.add(4) as *const i32);
                let micros = *(base.add(8) as *const i64);
                Some((months, days, micros))
            },
            VectorType::Dictionary => {
                let physical_idx = self.physical_index(idx);
                self.child.as_ref().unwrap().get_interval(physical_idx)
            }
            _ => None,
        }
    }
    // --- Setters ---

    /// Set value at index from a Value object.
    pub fn set_value(&mut self, idx: usize, val: &Value) {
        if self
            .try_set_scalar_value(idx, val)
            .expect("vector scalar value write failed")
        {
            return;
        }
        self.validity_mut().set_valid(idx);
        match val {
            Value::List(children, child_type) => {
                if !matches!(self.logical_type, LogicalType::List(_)) {
                    self.validity_mut().set_null(idx);
                    return;
                }

                fn write_list_entry(vector: &mut Vector, idx: usize, offset: u32, length: u32) {
                    let base = unsafe { vector.flat_data_mut::<u8>() };
                    let ptr = unsafe { base.add(idx * 8) as *mut u32 };
                    unsafe {
                        std::ptr::write_unaligned(ptr, offset);
                        std::ptr::write_unaligned(ptr.add(1), length);
                    }
                }

                let child = self.child.get_or_insert_with(|| {
                    Arc::new(
                        Vector::try_new(
                            child_type.clone(),
                            children.len().max(1),
                            self.buffer.allocator().clone(),
                        )
                        .expect("vector allocation failed"),
                    )
                });

                let dest_offset = child.len();
                let required = dest_offset + children.len();
                {
                    let child_mut = Arc::make_mut(child);
                    if required > child_mut.capacity() {
                        let new_capacity =
                            required.max(child_mut.capacity().saturating_mul(2)).max(1);
                        let mut new_child = Vector::try_new(
                            child_type.clone(),
                            new_capacity,
                            child_mut.allocator().clone(),
                        )
                        .expect("vector allocation failed");
                        new_child
                            .try_copy_range(0, child_mut, 0, dest_offset)
                            .expect("vector value list child copy allocation failed");
                        new_child.set_count(dest_offset);
                        *child_mut = new_child;
                    }
                }

                let child_mut = Arc::make_mut(child);
                child_mut.set_count(required);
                for (i, child_val) in children.iter().enumerate() {
                    child_mut.set_value(dest_offset + i, child_val);
                }

                if dest_offset > u32::MAX as usize || children.len() > u32::MAX as usize {
                    panic!("List entry exceeds u32 range");
                }
                write_list_entry(self, idx, dest_offset as u32, children.len() as u32);
                self.validity_mut().set_valid(idx);
            }
            Value::Struct(values, _fields) => {
                if let LogicalType::Struct(field_defs) = &self.logical_type {
                    if values.len() != field_defs.len() || self.children.len() != values.len() {
                        self.validity_mut().set_null(idx);
                        return;
                    }
                    for (child_arc, child_val) in self.children.iter_mut().zip(values.iter()) {
                        let child = Arc::make_mut(child_arc);
                        child.set_value(idx, child_val);
                    }
                } else {
                    self.validity_mut().set_null(idx);
                }
            }
            Value::Array(children, _, array_size) => {
                // Set array elements in the child vector.
                if let Some(child_arc) = &mut self.child {
                    let child = Arc::make_mut(child_arc);
                    let offset = idx * array_size;
                    if val.is_null() {
                        // Set all child elements to null
                        for i in 0..*array_size {
                            child.set_null(offset + i, true);
                        }
                    } else {
                        for (i, child_val) in children.iter().enumerate() {
                            child.set_value(offset + i, child_val);
                        }
                    }
                }
            }
            _ => unreachable!("scalar values are handled by try_set_scalar_value"),
        }
    }

    /// Try to write a scalar runtime value at `idx`.
    ///
    /// Returns `Ok(false)` for nested values so callers can route those through
    /// their dedicated vector kernels. Variable-length allocation failures are
    /// propagated to the caller.
    pub fn try_set_scalar_value(&mut self, idx: usize, val: &Value) -> Result<bool> {
        match val {
            Value::Boolean(v) => self.set_bool(idx, *v),
            Value::TinyInt(v) => self.set_i8(idx, *v),
            Value::SmallInt(v) => self.set_i16(idx, *v),
            Value::Integer(v) => self.set_i32(idx, *v),
            Value::BigInt(v) => self.set_i64(idx, *v),
            Value::HugeInt(v) => self.set_i128(idx, *v),
            Value::UTinyInt(v) => self.set_u8(idx, *v),
            Value::USmallInt(v) => self.set_u16(idx, *v),
            Value::UInteger(v) => self.set_u32(idx, *v),
            Value::UBigInt(v) => self.set_u64(idx, *v),
            Value::UHugeInt(v) => self.set_u128(idx, *v),
            Value::Uuid(v) => self.set_u128(idx, *v),
            Value::Float(v) => self.set_f32(idx, *v),
            Value::Double(v) => self.set_f64(idx, *v),
            Value::Decimal(v, precision, _scale) => {
                if *precision <= 18 {
                    let narrow = i64::try_from(*v).map_err(|_| {
                        paro_error::internal("Decimal value exceeds i64 range for precision")
                    })?;
                    self.set_i64(idx, narrow);
                } else {
                    self.set_i128(idx, *v);
                }
            }
            Value::Varchar(v) => self.try_set_string(idx, v)?,
            Value::Blob(v) => self.try_set_blob(idx, v)?,
            Value::Date(v) => self.set_i32(idx, *v),
            Value::Timestamp(v) => self.set_i64(idx, *v),
            Value::TimestampTz(v) => self.set_i64(idx, *v),
            Value::Time(v) => self.set_i64(idx, *v),
            Value::Interval(months, days, micros) => {
                // Interval is stored as: months (i32), days (i32), micros (i64) = 16 bytes
                unsafe {
                    let base = self.buffer.data().add(idx * 16);
                    *(base as *mut i32) = *months;
                    *(base.add(4) as *mut i32) = *days;
                    *(base.add(8) as *mut i64) = *micros;
                }
                self.validity_mut().set_valid(idx);
            }
            Value::Null(_) => self.try_set_null(idx, true)?,
            Value::List(..) | Value::Struct(..) | Value::Array(..) => {
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// Set i64 value at index.
    pub fn set_i64(&mut self, idx: usize, val: i64) {
        unsafe { self.set_flat(idx, val) };
        self.validity_mut().set_valid(idx);
    }

    /// Set i32 value at index.
    pub fn set_i32(&mut self, idx: usize, val: i32) {
        unsafe { self.set_flat(idx, val) };
        self.validity_mut().set_valid(idx);
    }

    /// Set i16 value at index.
    pub fn set_i16(&mut self, idx: usize, val: i16) {
        unsafe { self.set_flat(idx, val) };
        self.validity_mut().set_valid(idx);
    }

    /// Set i8 value at index.
    pub fn set_i8(&mut self, idx: usize, val: i8) {
        unsafe { self.set_flat(idx, val) };
        self.validity_mut().set_valid(idx);
    }

    /// Set i128 value at index.
    pub fn set_i128(&mut self, idx: usize, val: i128) {
        unsafe { self.set_flat(idx, val) };
        self.validity_mut().set_valid(idx);
    }

    /// Set u64 value at index.
    pub fn set_u64(&mut self, idx: usize, val: u64) {
        unsafe { self.set_flat(idx, val) };
        self.validity_mut().set_valid(idx);
    }

    /// Set u32 value at index.
    pub fn set_u32(&mut self, idx: usize, val: u32) {
        unsafe { self.set_flat(idx, val) };
        self.validity_mut().set_valid(idx);
    }

    /// Set u16 value at index.
    pub fn set_u16(&mut self, idx: usize, val: u16) {
        unsafe { self.set_flat(idx, val) };
        self.validity_mut().set_valid(idx);
    }

    /// Set u8 value at index.
    pub fn set_u8(&mut self, idx: usize, val: u8) {
        unsafe { self.set_flat(idx, val) };
        self.validity_mut().set_valid(idx);
    }

    /// Set u128 value at index.
    pub fn set_u128(&mut self, idx: usize, val: u128) {
        unsafe { self.set_flat(idx, val) };
        self.validity_mut().set_valid(idx);
    }

    /// Set f32 value at index.
    pub fn set_f32(&mut self, idx: usize, val: f32) {
        unsafe { self.set_flat(idx, val) };
        self.validity_mut().set_valid(idx);
    }

    /// Set f64 value at index.
    pub fn set_f64(&mut self, idx: usize, val: f64) {
        unsafe { self.set_flat(idx, val) };
        self.validity_mut().set_valid(idx);
    }

    /// Set bool value at index.
    pub fn set_bool(&mut self, idx: usize, val: bool) {
        unsafe { self.set_flat(idx, val) };
        self.validity_mut().set_valid(idx);
    }

    /// Set string value at index.
    ///
    /// For short strings (≤12 bytes), data is stored inline in StringView.
    /// For longer strings, data is stored in StringHeap and StringView.ptr points to it.
    pub fn set_string(&mut self, idx: usize, val: &str) {
        self.try_set_string(idx, val)
            .expect("vector string allocation failed");
    }

    /// Set string value at index.
    ///
    /// For short strings (≤12 bytes), data is stored inline in StringView.
    /// For longer strings, data is stored in StringHeap and StringView.ptr points to it.
    pub fn try_set_string(&mut self, idx: usize, val: &str) -> Result<()> {
        if !self.logical_type.is_utf8_varlen() {
            return Err(paro_error::type_mismatch(format!(
                "string write requires a textual varlen vector, got {:?}",
                self.logical_type
            )));
        }
        self.try_set_varlen(idx, val.as_bytes())
    }

    /// Set blob value at index.
    ///
    /// For short blobs (≤12 bytes), data is stored inline in StringView.
    /// For longer blobs, data is stored in StringHeap and StringView.ptr points to it.
    pub fn set_blob(&mut self, idx: usize, val: &[u8]) {
        self.try_set_blob(idx, val)
            .expect("vector blob allocation failed");
    }

    /// Set blob value at index.
    ///
    /// For short blobs (≤12 bytes), data is stored inline in StringView.
    /// For longer blobs, data is stored in StringHeap and StringView.ptr points to it.
    pub fn try_set_blob(&mut self, idx: usize, val: &[u8]) -> Result<()> {
        if self.logical_type != LogicalType::Blob {
            return Err(paro_error::type_mismatch(format!(
                "blob write requires a BLOB vector, got {:?}",
                self.logical_type
            )));
        }
        self.try_set_varlen(idx, val)
    }

    fn try_set_varlen(&mut self, idx: usize, val: &[u8]) -> Result<()> {
        if idx >= self.buffer.capacity() {
            return Err(paro_error::internal(format!(
                "varlen write index out of bounds: idx={idx}, capacity={}",
                self.buffer.capacity()
            )));
        }

        self.try_make_exclusive()?;
        self.validity.try_make_exclusive()?;

        let inline_value = if let Some(value) = StringView::try_inline(val) {
            value
        } else {
            self.try_add_out_of_line_varlen(idx, val)?
        };

        unsafe { self.set_flat(idx, inline_value) };
        if self.validity.is_mask_set() {
            self.validity.set_valid_unsafe(idx);
        }
        Ok(())
    }

    fn try_add_out_of_line_varlen(&mut self, idx: usize, val: &[u8]) -> Result<StringView> {
        if let Some(heap) = self.string_heap.as_mut().and_then(Arc::get_mut) {
            // SAFETY: `heap` is retained by `self`, which also stores the view.
            return unsafe { heap.try_add_blob(val) };
        }

        let preserve_entries = self
            .buffer
            .capacity()
            .min(self.count.max(idx.saturating_add(1)));
        let allocator = self.allocator().clone();
        let old_heap = self.string_heap.clone();
        let initial_capacity = old_heap
            .as_ref()
            .map(|heap| heap.allocation_size())
            .unwrap_or(0)
            .max(preserve_entries)
            .max(idx.saturating_add(1))
            .max(1);

        let mut rebuilt_heap = StringHeap::with_allocator(initial_capacity, allocator.clone());
        let rebuilt_buffer =
            VectorBuffer::try_with_allocator(StringView::SIZE, self.buffer.capacity(), allocator)?;

        unsafe {
            let entries = self.buffer.data() as *const StringView;
            let rewritten_entries = rebuilt_buffer.data() as *mut StringView;
            for entry_idx in 0..preserve_entries {
                let entry = *entries.add(entry_idx);
                // SAFETY: `rebuilt_heap` becomes the owner of the rewritten buffer.
                *rewritten_entries.add(entry_idx) = rebuilt_heap.try_add_blob(entry.as_bytes())?;
            }
        }

        // SAFETY: `rebuilt_heap` becomes the owner of the rewritten buffer.
        let inline_value = unsafe { rebuilt_heap.try_add_blob(val) }?;

        self.buffer = rebuilt_buffer;
        self.string_heap = Some(Arc::new(rebuilt_heap));
        Ok(inline_value)
    }

    /// Slice a source vector into this vector by materializing the requested range.
    pub fn try_slice(&mut self, source: &Vector, start: usize, end: usize) -> Result<()> {
        let count = end.checked_sub(start).ok_or_else(|| {
            paro_error::internal(format!(
                "vector slice end before start: start={start}, end={end}"
            ))
        })?;
        if end > source.len() {
            return Err(paro_error::internal(format!(
                "vector slice out of bounds: end={end}, source_len={}",
                source.len()
            )));
        }

        self.try_copy_range(0, source, start, count)?;
        self.try_set_count(count)
    }
}
