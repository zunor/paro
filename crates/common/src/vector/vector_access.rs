// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::{StringHeap, ValidityMask, Vector, VectorType};
use crate::runtime_value::Value;
use crate::types::{InlineString, LogicalType};
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
            | LogicalType::Jsonb => Value::Varchar(self.get_string(idx).unwrap().to_string()),
            LogicalType::Blob => Value::Blob(self.get_blob(idx).unwrap().to_vec()),
            LogicalType::Array(child_type, array_size) => {
                fn resolve_array_row(vector: &Vector, idx: usize) -> (&Vector, usize) {
                    match vector.vector_type() {
                        VectorType::Flat => (vector, idx),
                        VectorType::Constant => (vector, 0),
                        VectorType::Dictionary => {
                            let sel = vector
                                .sel_vector()
                                .expect("Dictionary vector missing selection vector");
                            let child = vector.child().expect("Dictionary vector missing child");
                            resolve_array_row(child, sel.get(idx))
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
                            let sel = vector
                                .sel_vector()
                                .expect("Dictionary vector missing selection vector");
                            let child = vector.child().expect("Dictionary vector missing child");
                            resolve_collection_row(child, sel.get(idx))
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
                            let sel = vector
                                .sel_vector()
                                .expect("Dictionary vector missing selection vector");
                            let child = vector.child().expect("Dictionary vector missing child");
                            resolve_struct_row(child, sel.get(idx))
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
                let physical_idx = self.sel_vector.as_ref().unwrap().get(idx);
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
                let physical_idx = self.sel_vector.as_ref().unwrap().get(idx);
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
                let physical_idx = self.sel_vector.as_ref().unwrap().get(idx);
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
                let physical_idx = self.sel_vector.as_ref().unwrap().get(idx);
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
                let physical_idx = self.sel_vector.as_ref().unwrap().get(idx);
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
                let physical_idx = self.sel_vector.as_ref().unwrap().get(idx);
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
                let physical_idx = self.sel_vector.as_ref().unwrap().get(idx);
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
                let physical_idx = self.sel_vector.as_ref().unwrap().get(idx);
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
                let physical_idx = self.sel_vector.as_ref().unwrap().get(idx);
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
                let physical_idx = self.sel_vector.as_ref().unwrap().get(idx);
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
                let physical_idx = self.sel_vector.as_ref().unwrap().get(idx);
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
                let physical_idx = self.sel_vector.as_ref().unwrap().get(idx);
                self.child.as_ref().unwrap().get_f64(physical_idx)
            }
            _ => None,
        }
    }

    /// Get string value at index. Returns None if null.
    pub fn get_string(&self, idx: usize) -> Option<&str> {
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
                // SAFETY: We know buffer contains InlineString array
                let inline_str = unsafe {
                    let entries = self.buffer.data() as *const InlineString;
                    &*entries.add(entry_idx)
                };
                Some(inline_str.as_str())
            }
            VectorType::Dictionary => {
                let physical_idx = self.sel_vector.as_ref()?.get(idx);
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
                // SAFETY: We know buffer contains InlineString array
                let inline_str = unsafe {
                    let entries = self.buffer.data() as *const InlineString;
                    &*entries.add(entry_idx)
                };
                Some(inline_str.as_bytes())
            }
            VectorType::Dictionary => {
                let physical_idx = self.sel_vector.as_ref()?.get(idx);
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
                let physical_idx = self.sel_vector.as_ref().unwrap().get(idx);
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
                let physical_idx = self.sel_vector.as_ref().unwrap().get(idx);
                self.child.as_ref().unwrap().get_interval(physical_idx)
            }
            _ => None,
        }
    }
    // --- Setters ---

    /// Set value at index from a Value object.
    pub fn set_value(&mut self, idx: usize, val: &Value) {
        if val.is_null() {
            self.validity_mut().set_null(idx);
            return;
        }
        self.validity_mut().set_valid(idx);
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
                    let narrow =
                        i64::try_from(*v).expect("Decimal value exceeds i64 range for precision");
                    self.set_i64(idx, narrow);
                } else {
                    self.set_i128(idx, *v);
                }
            }
            Value::Varchar(v) => self.set_string(idx, v),
            Value::Blob(v) => self.set_blob(idx, v),
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
            }
            Value::Null(_) => self.validity_mut().set_null(idx),
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
                    Arc::new(Vector::with_capacity_and_allocator(
                        child_type.clone(),
                        children.len().max(1),
                        self.buffer.allocator().clone(),
                    ))
                });

                let dest_offset = child.len();
                let required = dest_offset + children.len();
                {
                    let child_mut = Arc::make_mut(child);
                    if required > child_mut.capacity() {
                        let new_capacity =
                            required.max(child_mut.capacity().saturating_mul(2)).max(1);
                        let mut new_child = Vector::with_capacity_and_allocator(
                            child_type.clone(),
                            new_capacity,
                            child_mut.allocator().clone(),
                        );
                        new_child.set_count(dest_offset);
                        for i in 0..dest_offset {
                            new_child.copy_at(i, child_mut, i);
                        }
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
        }
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
    /// For short strings (≤12 bytes), data is stored inline in InlineString.
    /// For longer strings, data is stored in StringHeap and InlineString.ptr points to it.
    pub fn set_string(&mut self, idx: usize, val: &str) {
        self.make_exclusive();

        // Ensure we have exclusive access to the string heap
        let heap = match &mut self.string_heap {
            Some(arc) => {
                // Try to get mutable reference if we're the only owner
                match Arc::get_mut(arc) {
                    Some(h) => h,
                    None => {
                        // Multiple owners: create a new heap (expensive but correct)
                        self.string_heap = Some(Arc::new(StringHeap::new()));
                        Arc::get_mut(self.string_heap.as_mut().unwrap()).unwrap()
                    }
                }
            }
            None => {
                self.string_heap = Some(Arc::new(StringHeap::new()));
                Arc::get_mut(self.string_heap.as_mut().unwrap()).unwrap()
            }
        };

        // add_string handles both short (inlined) and long (heap) strings
        let inline_str = heap.add_string(val);
        unsafe { self.set_flat(idx, inline_str) };
        self.validity_mut().set_valid(idx);
    }

    /// Set blob value at index.
    ///
    /// For short blobs (≤12 bytes), data is stored inline in InlineString.
    /// For longer blobs, data is stored in StringHeap and InlineString.ptr points to it.
    pub fn set_blob(&mut self, idx: usize, val: &[u8]) {
        self.make_exclusive();

        // Ensure we have exclusive access to the string heap
        let heap = match &mut self.string_heap {
            Some(arc) => {
                // Try to get mutable reference if we're the only owner
                match Arc::get_mut(arc) {
                    Some(h) => h,
                    None => {
                        // Multiple owners: create a new heap (expensive but correct)
                        self.string_heap = Some(Arc::new(StringHeap::new()));
                        Arc::get_mut(self.string_heap.as_mut().unwrap()).unwrap()
                    }
                }
            }
            None => {
                self.string_heap = Some(Arc::new(StringHeap::new()));
                Arc::get_mut(self.string_heap.as_mut().unwrap()).unwrap()
            }
        };

        // add_blob handles both short (inlined) and long (heap) blobs
        let inline_str = heap.add_blob(val);
        unsafe { self.set_flat(idx, inline_str) };
        self.validity_mut().set_valid(idx);
    }

    /// Slice a source vector into this vector.
    /// This is an MVP implementation that copies data.
    pub fn slice(&mut self, source: &Vector, start: usize, end: usize) {
        let count = end - start;

        // Handle Array type specially - need to slice child vector with multiplied offset
        if let LogicalType::Array(_, array_size) = &self.logical_type {
            if let (Some(self_child), Some(source_child)) = (&mut self.child, &source.child) {
                let child_start = start * array_size;
                let child_end = end * array_size;
                let self_child_mut = Arc::make_mut(self_child);
                self_child_mut.slice(source_child, child_start, child_end);
            }
            // Copy validity for the array elements
            for i in 0..count {
                if source.is_null(start + i) {
                    self.validity_mut().set_null(i);
                } else {
                    self.validity_mut().set_valid(i);
                }
            }
            self.count = count;
            return;
        }

        // Default implementation for non-Array types
        for i in 0..count {
            self.copy_at(i, source, start + i);
        }
        self.count = count;
    }

    /// Reference a single value, converting this vector to a constant vector.
    ///
    /// The vector will be converted to a CONSTANT vector that references
    /// the given value for all logical indices.
    ///
    /// # Arguments
    /// * `value` - The value to reference
    pub fn reference_value(&mut self, value: &Value) {
        use crate::allocator::default_allocator;

        let allocator = Arc::new(default_allocator());
        let logical_type = value.logical_type();

        if value.is_null() {
            // Create a constant null vector
            self.vector_type = VectorType::Constant;
            self.logical_type = logical_type;
            self.buffer = super::VectorBuffer::with_allocator(0, 0, allocator);
            // For constant vectors, validity mask only needs 1 entry
            self.validity = ValidityMask::with_allocator(1, self.buffer.allocator().clone());
            self.validity.set_null(0);
            self.sel_vector = None;
            self.child = None;
            self.string_heap = None;
            self.dictionary_info = None;
            return;
        }

        // Create a constant vector from the value
        match value {
            Value::Boolean(v) => {
                self.buffer = super::VectorBuffer::with_allocator(1, 1, allocator);
                unsafe {
                    let ptr = self.buffer.data() as *mut bool;
                    *ptr = *v;
                }
            }
            Value::TinyInt(v) => {
                self.buffer = super::VectorBuffer::with_allocator(1, 1, allocator);
                unsafe {
                    let ptr = self.buffer.data() as *mut i8;
                    *ptr = *v;
                }
            }
            Value::SmallInt(v) => {
                self.buffer = super::VectorBuffer::with_allocator(2, 1, allocator);
                unsafe {
                    let ptr = self.buffer.data() as *mut i16;
                    *ptr = *v;
                }
            }
            Value::Integer(v) => {
                self.buffer = super::VectorBuffer::with_allocator(4, 1, allocator);
                unsafe {
                    let ptr = self.buffer.data() as *mut i32;
                    *ptr = *v;
                }
            }
            Value::BigInt(v) => {
                self.buffer = super::VectorBuffer::with_allocator(8, 1, allocator);
                unsafe {
                    let ptr = self.buffer.data() as *mut i64;
                    *ptr = *v;
                }
            }
            Value::HugeInt(v) => {
                self.buffer = super::VectorBuffer::with_allocator(16, 1, allocator);
                unsafe {
                    let ptr = self.buffer.data() as *mut i128;
                    *ptr = *v;
                }
            }
            Value::UTinyInt(v) => {
                self.buffer = super::VectorBuffer::with_allocator(1, 1, allocator);
                unsafe {
                    let ptr = self.buffer.data();
                    *ptr = *v;
                }
            }
            Value::USmallInt(v) => {
                self.buffer = super::VectorBuffer::with_allocator(2, 1, allocator);
                unsafe {
                    let ptr = self.buffer.data() as *mut u16;
                    *ptr = *v;
                }
            }
            Value::UInteger(v) => {
                self.buffer = super::VectorBuffer::with_allocator(4, 1, allocator);
                unsafe {
                    let ptr = self.buffer.data() as *mut u32;
                    *ptr = *v;
                }
            }
            Value::UBigInt(v) => {
                self.buffer = super::VectorBuffer::with_allocator(8, 1, allocator);
                unsafe {
                    let ptr = self.buffer.data() as *mut u64;
                    *ptr = *v;
                }
            }
            Value::UHugeInt(v) => {
                self.buffer = super::VectorBuffer::with_allocator(16, 1, allocator);
                unsafe {
                    let ptr = self.buffer.data() as *mut u128;
                    *ptr = *v;
                }
            }
            Value::Uuid(v) => {
                self.buffer = super::VectorBuffer::with_allocator(16, 1, allocator);
                unsafe {
                    let ptr = self.buffer.data() as *mut u128;
                    *ptr = *v;
                }
            }
            Value::Float(v) => {
                self.buffer = super::VectorBuffer::with_allocator(4, 1, allocator);
                unsafe {
                    let ptr = self.buffer.data() as *mut f32;
                    *ptr = *v;
                }
            }
            Value::Double(v) => {
                self.buffer = super::VectorBuffer::with_allocator(8, 1, allocator);
                unsafe {
                    let ptr = self.buffer.data() as *mut f64;
                    *ptr = *v;
                }
            }
            Value::Decimal(v, precision, _scale) => {
                if *precision <= 18 {
                    self.buffer = super::VectorBuffer::with_allocator(8, 1, allocator);
                    let narrow =
                        i64::try_from(*v).expect("Decimal value exceeds i64 range for precision");
                    unsafe {
                        let ptr = self.buffer.data() as *mut i64;
                        *ptr = narrow;
                    }
                } else {
                    self.buffer = super::VectorBuffer::with_allocator(16, 1, allocator);
                    unsafe {
                        let ptr = self.buffer.data() as *mut i128;
                        *ptr = *v;
                    }
                }
            }
            Value::Varchar(v) => {
                self.buffer = super::VectorBuffer::with_allocator(
                    std::mem::size_of::<InlineString>(),
                    1,
                    allocator.clone(),
                );

                // Use StringHeap which handles both short and long strings
                let mut heap = StringHeap::with_allocator(v.len().max(64), allocator.clone());
                let inline_str = heap.add_string(v);
                unsafe {
                    let ptr = self.buffer.data() as *mut InlineString;
                    *ptr = inline_str;
                }
                // Only store heap if string is not inlined
                if !inline_str.is_inlined() {
                    self.string_heap = Some(Arc::new(heap));
                }
            }
            Value::Blob(v) => {
                self.buffer = super::VectorBuffer::with_allocator(
                    std::mem::size_of::<InlineString>(),
                    1,
                    allocator.clone(),
                );

                // Use StringHeap which handles both short and long blobs
                let mut heap = StringHeap::with_allocator(v.len().max(64), allocator.clone());
                let inline_str = heap.add_blob(v);
                unsafe {
                    let ptr = self.buffer.data() as *mut InlineString;
                    *ptr = inline_str;
                }
                // Only store heap if blob is not inlined
                if !inline_str.is_inlined() {
                    self.string_heap = Some(Arc::new(heap));
                }
            }
            Value::Date(v) => {
                self.buffer = super::VectorBuffer::with_allocator(4, 1, allocator);
                unsafe {
                    let ptr = self.buffer.data() as *mut i32;
                    *ptr = *v;
                }
            }
            Value::Timestamp(v) | Value::TimestampTz(v) | Value::Time(v) => {
                self.buffer = super::VectorBuffer::with_allocator(8, 1, allocator);
                unsafe {
                    let ptr = self.buffer.data() as *mut i64;
                    *ptr = *v;
                }
            }
            Value::Interval(months, days, micros) => {
                self.buffer = super::VectorBuffer::with_allocator(16, 1, allocator);
                unsafe {
                    let base = self.buffer.data();
                    *(base as *mut i32) = *months;
                    *(base.add(4) as *mut i32) = *days;
                    *(base.add(8) as *mut i64) = *micros;
                }
            }
            Value::Null(_) => {
                // Already handled above
                unreachable!()
            }
            Value::List(_, _) => {
                // List values are not directly supported as constants.
                // Mark as null for now.
                self.validity = ValidityMask::with_allocator(1, self.buffer.allocator().clone());
                self.validity.set_null(0);
            }
            Value::Array(children, child_type, array_size) => {
                // Create a child vector with the array elements
                let child_capacity = *array_size;
                let mut child = Vector::with_capacity_and_allocator(
                    child_type.clone(),
                    child_capacity,
                    allocator.clone(),
                );
                child.set_count(child_capacity);

                // Set each element in the child vector
                for (i, child_val) in children.iter().enumerate() {
                    child.set_value(i, child_val);
                }

                self.child = Some(Arc::new(child));
                self.buffer = super::VectorBuffer::with_allocator(0, 0, allocator);
                self.validity = ValidityMask::with_allocator(1, self.buffer.allocator().clone());
            }
            Value::Struct(values, fields) => {
                let mut children = Vec::with_capacity(values.len());
                for (value, (_name, field_type)) in values.iter().zip(fields.iter()) {
                    let child = Vector::constant_from_value(field_type.clone(), value.clone(), 1);
                    children.push(Arc::new(child));
                }
                self.children = children;
                self.child = None;
                self.buffer = super::VectorBuffer::with_allocator(0, 0, allocator);
                self.validity = ValidityMask::with_allocator(1, self.buffer.allocator().clone());
            }
        }

        self.vector_type = VectorType::Constant;
        self.logical_type = logical_type;
        if !matches!(
            value,
            Value::Varchar(_)
                | Value::Blob(_)
                | Value::List(_, _)
                | Value::Array(_, _, _)
                | Value::Struct(_, _)
        ) {
            // For constant vectors, validity mask only needs 1 entry
            self.validity = ValidityMask::with_allocator(1, self.buffer.allocator().clone());
        }
        self.sel_vector = None;
        self.dictionary_info = None;
        // Only reset child if we're NOT an Array or List (which just set it)
        if !matches!(value, Value::Array(_, _, _) | Value::List(_, _)) {
            self.child = None;
        }
        if !matches!(value, Value::Struct(_, _)) {
            self.children.clear();
        }
    }
}
