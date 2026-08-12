// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::{
    DictionaryInfo, DictionarySource, SelectionVector, StringHeap, ValidatedVectorSelection,
    ValidityMask, Vector, VectorBuffer, VectorSelection, VectorType,
};
use crate::allocator::Allocator;
use crate::error::{self as paro_error, Result};
use crate::runtime_value::Value;
use crate::types::{InlineString, LogicalType};
use std::sync::Arc;

impl Vector {
    /// Create a flat vector from i64 values with allocator.
    pub fn try_from_i64(values: &[i64], allocator: Arc<dyn Allocator>) -> Result<Self> {
        let mut vec = Self::try_new(LogicalType::BigInt, values.len(), allocator)?;
        vec.count = values.len();
        vec.validity = ValidityMask::with_allocator(values.len(), vec.buffer.allocator().clone());

        // SAFETY: We know the type is i64
        unsafe {
            let ptr = vec.buffer.data() as *mut i64;
            std::ptr::copy_nonoverlapping(values.as_ptr(), ptr, values.len());
        }
        Ok(vec)
    }

    /// Create a flat vector from i32 values with allocator.
    pub fn try_from_i32(values: &[i32], allocator: Arc<dyn Allocator>) -> Result<Self> {
        let mut vec = Self::try_new(LogicalType::Integer, values.len(), allocator)?;
        vec.count = values.len();
        vec.validity = ValidityMask::with_allocator(values.len(), vec.buffer.allocator().clone());

        // SAFETY: We know the type is i32
        unsafe {
            let ptr = vec.buffer.data() as *mut i32;
            std::ptr::copy_nonoverlapping(values.as_ptr(), ptr, values.len());
        }
        Ok(vec)
    }

    /// Create a flat vector from f64 values with allocator.
    pub fn try_from_f64(values: &[f64], allocator: Arc<dyn Allocator>) -> Result<Self> {
        let mut vec = Self::try_new(LogicalType::Double, values.len(), allocator)?;
        vec.count = values.len();
        vec.validity = ValidityMask::with_allocator(values.len(), vec.buffer.allocator().clone());

        // SAFETY: We know the type is f64
        unsafe {
            let ptr = vec.buffer.data() as *mut f64;
            std::ptr::copy_nonoverlapping(values.as_ptr(), ptr, values.len());
        }
        Ok(vec)
    }

    /// Create a flat vector from f32 values with allocator.
    pub fn try_from_f32(values: &[f32], allocator: Arc<dyn Allocator>) -> Result<Self> {
        let mut vec = Self::try_new(LogicalType::Float, values.len(), allocator)?;
        vec.count = values.len();
        vec.validity = ValidityMask::with_allocator(values.len(), vec.buffer.allocator().clone());

        // SAFETY: We know the type is f32
        unsafe {
            let ptr = vec.buffer.data() as *mut f32;
            std::ptr::copy_nonoverlapping(values.as_ptr(), ptr, values.len());
        }
        Ok(vec)
    }

    /// Create a flat vector from bool values with allocator.
    pub fn try_from_bool(values: &[bool], allocator: Arc<dyn Allocator>) -> Result<Self> {
        let mut vec = Self::try_new(LogicalType::Boolean, values.len(), allocator)?;
        vec.count = values.len();
        vec.validity = ValidityMask::with_allocator(values.len(), vec.buffer.allocator().clone());

        // SAFETY: We know the type is bool
        unsafe {
            let ptr = vec.buffer.data() as *mut bool;
            std::ptr::copy_nonoverlapping(values.as_ptr(), ptr, values.len());
        }
        Ok(vec)
    }

    /// Create a flat vector from nullable bool values with allocator.
    pub fn try_from_nullable_bools(
        values: &[Option<bool>],
        allocator: Arc<dyn Allocator>,
    ) -> Result<Self> {
        let mut vec = Self::try_new(LogicalType::Boolean, values.len(), allocator)?;
        vec.count = values.len();
        vec.validity = ValidityMask::with_allocator(values.len(), vec.buffer.allocator().clone());

        unsafe {
            let ptr = vec.buffer.data() as *mut bool;
            for (i, val) in values.iter().enumerate() {
                match val {
                    Some(v) => *ptr.add(i) = *v,
                    None => vec.validity.set_null(i),
                }
            }
        }
        Ok(vec)
    }

    /// Create a flat vector from nullable u64 values (mapped to BigInt) with allocator.
    pub fn try_from_nullable_u64(
        values: &[Option<u64>],
        allocator: Arc<dyn Allocator>,
    ) -> Result<Self> {
        let mut vec = Self::try_new(LogicalType::BigInt, values.len(), allocator)?;
        vec.count = values.len();
        vec.validity = ValidityMask::with_allocator(values.len(), vec.buffer.allocator().clone());

        unsafe {
            let ptr = vec.buffer.data() as *mut i64;
            for (i, val) in values.iter().enumerate() {
                match val {
                    Some(v) => *ptr.add(i) = *v as i64,
                    None => vec.validity.set_null(i),
                }
            }
        }
        Ok(vec)
    }

    /// Create a flat vector from nullable string values with allocator.
    pub fn try_from_nullable_strings(
        values: &[Option<&str>],
        allocator: Arc<dyn Allocator>,
    ) -> Result<Self> {
        let mut vec = Self::try_new(LogicalType::Varchar, values.len(), allocator.clone())?;
        vec.count = values.len();
        vec.validity = ValidityMask::with_allocator(values.len(), vec.buffer.allocator().clone());

        // Create heap for string storage
        let mut heap = StringHeap::with_allocator(4096, allocator.clone());

        let buffer = VectorBuffer::try_with_allocator(
            std::mem::size_of::<InlineString>(),
            values.len(),
            allocator,
        )?;

        // SAFETY: We allocated space for InlineString array
        unsafe {
            let entries = buffer.data() as *mut InlineString;
            for (i, s) in values.iter().enumerate() {
                match s {
                    Some(str_val) => {
                        // try_add_string handles both short (inlined) and long (heap) strings.
                        *entries.add(i) = heap.try_add_string(str_val)?;
                    }
                    None => {
                        vec.validity.set_null(i);
                        *entries.add(i) = InlineString::empty();
                    }
                }
            }
        }

        vec.buffer = buffer;
        // Only store heap if it has allocations
        if !heap.is_empty() {
            vec.string_heap = Some(Arc::new(heap));
        }
        Ok(vec)
    }

    /// Create a flat vector from strings with allocator.
    pub fn try_from_strings(values: &[&str], allocator: Arc<dyn Allocator>) -> Result<Self> {
        let mut vec = Self::try_new(LogicalType::Varchar, values.len(), allocator.clone())?;
        vec.count = values.len();
        vec.validity = ValidityMask::with_allocator(values.len(), vec.buffer.allocator().clone());

        // Create heap for string storage
        let mut heap = StringHeap::with_allocator(4096, allocator.clone());

        let buffer = VectorBuffer::try_with_allocator(
            std::mem::size_of::<InlineString>(),
            values.len(),
            allocator,
        )?;

        // SAFETY: We allocated space for InlineString array
        unsafe {
            let entries = buffer.data() as *mut InlineString;
            for (i, s) in values.iter().enumerate() {
                // try_add_string handles both short (inlined) and long (heap) strings.
                *entries.add(i) = heap.try_add_string(s)?;
            }
        }

        vec.buffer = buffer;
        // Only store heap if it has allocations
        if !heap.is_empty() {
            vec.string_heap = Some(Arc::new(heap));
        }
        Ok(vec)
    }

    /// Create an embedding vector with allocator.
    pub fn try_from_embeddings(
        embeddings: &[Vec<f32>],
        dimensions: usize,
        allocator: Arc<dyn Allocator>,
    ) -> Result<Self> {
        let count = embeddings.len();

        // Flatten all embeddings into a single child vector
        let flattened: Vec<f32> = embeddings.iter().flatten().copied().collect();
        let child = Self::try_from_f32(&flattened, allocator.clone())?;

        Ok(Self {
            vector_type: VectorType::Flat,
            logical_type: LogicalType::Array(Box::new(LogicalType::Float), dimensions),
            buffer: VectorBuffer::try_with_allocator(0, 0, allocator)?,
            validity: ValidityMask::with_allocator(count, child.buffer.allocator().clone()),
            count,
            selection: VectorSelection::None,
            child: Some(Arc::new(child)),
            children: Vec::new(),
            string_heap: None,
            dictionary_info: None,
            lifetime_owners: None,
        })
    }

    /// Create a constant vector with a single value and allocator.
    pub fn try_constant<T: Copy>(
        logical_type: LogicalType,
        value: T,
        count: usize,
        allocator: Arc<dyn Allocator>,
    ) -> Result<Self> {
        let element_size = std::mem::size_of::<T>();
        let buffer = VectorBuffer::try_with_allocator(element_size, 1, allocator)?;
        let validity_allocator = buffer.allocator().clone();

        // SAFETY: We just allocated space for one T
        unsafe {
            let ptr = buffer.data() as *mut T;
            *ptr = value;
        }

        Ok(Self {
            vector_type: VectorType::Constant,
            buffer,
            validity: ValidityMask::with_allocator(1, validity_allocator),
            count,
            logical_type,
            selection: VectorSelection::None,
            child: None,
            children: Vec::new(),
            string_heap: None,
            dictionary_info: None,
            lifetime_owners: None,
        })
    }

    /// Create a constant null vector with allocator.
    pub fn try_constant_null(
        logical_type: LogicalType,
        count: usize,
        allocator: Arc<dyn Allocator>,
    ) -> Result<Self> {
        let buffer = VectorBuffer::try_with_allocator(0, 0, allocator)?;
        let validity_allocator = buffer.allocator().clone();
        let mut vec = Self {
            vector_type: VectorType::Constant,
            buffer,
            validity: ValidityMask::with_allocator(1, validity_allocator),
            count,
            logical_type,
            selection: VectorSelection::None,
            child: None,
            children: Vec::new(),
            string_heap: None,
            dictionary_info: None,
            lifetime_owners: None,
        };
        vec.validity.set_null(0);
        Ok(vec)
    }

    /// Create a sequence vector with allocator.
    pub fn try_sequence(
        start: i64,
        increment: i64,
        count: usize,
        allocator: Arc<dyn Allocator>,
    ) -> Result<Self> {
        let buffer = VectorBuffer::try_with_allocator(std::mem::size_of::<i64>(), 2, allocator)?;
        let validity_allocator = buffer.allocator().clone();

        // SAFETY: We allocated space for 2 i64s
        unsafe {
            let ptr = buffer.data() as *mut i64;
            *ptr = start;
            *ptr.add(1) = increment;
        }

        Ok(Self {
            vector_type: VectorType::Sequence,
            buffer,
            validity: ValidityMask::with_allocator(count, validity_allocator),
            count,
            logical_type: LogicalType::BigInt,
            selection: VectorSelection::None,
            child: None,
            children: Vec::new(),
            string_heap: None,
            dictionary_info: None,
            lifetime_owners: None,
        })
    }

    fn try_dictionary_with_info(
        child: Arc<Vector>,
        selection: VectorSelection,
        dictionary_info: DictionaryInfo,
        selection_is_validated: bool,
    ) -> Result<Self> {
        if !selection_is_validated {
            selection.validate_child_bounds(child.len())?;
        }
        let (base_child, combined_selection) = if child.vector_type == VectorType::Dictionary {
            let base_child = child
                .child
                .as_ref()
                .expect("Dictionary vector missing child")
                .clone();
            let merged_selection = child.selection.try_compose(selection)?;
            (base_child, merged_selection)
        } else {
            (child, selection)
        };

        let allocator = base_child.buffer.allocator().clone();
        let count = combined_selection.len();
        let validity_allocator = base_child.buffer.allocator().clone();
        Ok(Self {
            vector_type: VectorType::Dictionary,
            logical_type: base_child.logical_type.clone(),
            buffer: VectorBuffer::try_with_allocator(0, 0, allocator)?,
            validity: ValidityMask::with_allocator(count, validity_allocator),
            count,
            selection: combined_selection,
            child: Some(base_child),
            children: Vec::new(),
            string_heap: None,
            dictionary_info: Some(dictionary_info),
            lifetime_owners: None,
        })
    }

    /// Create a canonicalized dictionary vector that represents a generic selection overlay.
    pub fn try_dictionary<S>(child: Arc<Vector>, selection: S) -> Result<Self>
    where
        S: Into<SelectionVector>,
    {
        let unique_len = if child.vector_type == VectorType::Dictionary {
            child
                .child
                .as_ref()
                .expect("Dictionary vector missing child")
                .len()
        } else {
            child.len()
        };
        Self::try_dictionary_with_info(
            child,
            VectorSelection::Materialized(selection.into()),
            DictionaryInfo {
                unique_len,
                provenance_id: None,
                source: DictionarySource::GenericSelection,
            },
            false,
        )
    }

    /// Create a generic dictionary overlay from a reusable bounds proof.
    pub fn try_dictionary_from_validated(
        child: Arc<Vector>,
        selection: ValidatedVectorSelection,
    ) -> Result<Self> {
        if selection.child_count != child.len() {
            return Err(paro_error::invalid_input(format!(
                "validated dictionary selection targets {} rows, child has {}",
                selection.child_count,
                child.len()
            )));
        }
        let unique_len = if child.vector_type == VectorType::Dictionary {
            child
                .child
                .as_ref()
                .expect("Dictionary vector missing child")
                .len()
        } else {
            child.len()
        };
        Self::try_dictionary_with_info(
            child,
            selection.selection,
            DictionaryInfo {
                unique_len,
                provenance_id: None,
                source: DictionarySource::GenericSelection,
            },
            true,
        )
    }

    /// Create a dictionary vector with explicit provenance metadata.
    pub fn try_with_dictionary<S>(
        child: Arc<Vector>,
        selection: S,
        dictionary_info: DictionaryInfo,
    ) -> Result<Self>
    where
        S: Into<SelectionVector>,
    {
        Self::try_dictionary_with_info(
            child,
            VectorSelection::Materialized(selection.into()),
            dictionary_info,
            false,
        )
    }

    /// Create a dictionary vector with provenance from a reusable bounds proof.
    pub fn try_with_validated_dictionary(
        child: Arc<Vector>,
        selection: ValidatedVectorSelection,
        dictionary_info: DictionaryInfo,
    ) -> Result<Self> {
        if selection.child_count != child.len() {
            return Err(paro_error::invalid_input(format!(
                "validated dictionary selection targets {} rows, child has {}",
                selection.child_count,
                child.len()
            )));
        }
        Self::try_dictionary_with_info(child, selection.selection, dictionary_info, true)
    }

    /// Create a dictionary vector from a first-class selection representation.
    pub fn try_gather_ref<S>(child: Arc<Vector>, selection: S) -> Result<Self>
    where
        S: Into<VectorSelection>,
    {
        let unique_len = if child.vector_type == VectorType::Dictionary {
            child
                .child
                .as_ref()
                .expect("Dictionary vector missing child")
                .len()
        } else {
            child.len()
        };
        Self::try_dictionary_with_info(
            child,
            selection.into(),
            DictionaryInfo {
                unique_len,
                provenance_id: None,
                source: DictionarySource::GenericSelection,
            },
            false,
        )
    }

    /// Create a zero-copy range view over this vector.
    pub fn slice_ref(&self, offset: usize, len: usize) -> Result<Self> {
        if offset.checked_add(len).is_none_or(|end| end > self.len()) {
            return Err(paro_error::out_of_range(format!(
                "vector range offset={offset} length={len} exceeds cardinality {}",
                self.len()
            )));
        }
        match self.vector_type {
            VectorType::Constant => {
                let mut result = self.reference();
                result.count = len;
                Ok(result)
            }
            _ => {
                let dictionary_info = self.dictionary_info.clone().unwrap_or(DictionaryInfo {
                    unique_len: self.len(),
                    provenance_id: None,
                    source: DictionarySource::GenericSelection,
                });
                Self::try_dictionary_with_info(
                    Arc::new(self.reference()),
                    VectorSelection::Range { offset, count: len },
                    dictionary_info,
                    true,
                )
            }
        }
    }

    /// Create an array vector from flattened data.
    pub fn try_from_array(
        element_type: LogicalType,
        child: Arc<Vector>,
        count: usize,
        array_size: usize,
    ) -> Result<Self> {
        let allocator = child.buffer.allocator().clone();
        let validity_allocator = child.buffer.allocator().clone();
        let mut vec = Self {
            vector_type: VectorType::Flat,
            logical_type: LogicalType::Array(Box::new(element_type), array_size),
            buffer: VectorBuffer::try_with_allocator(0, 0, allocator)?,
            validity: ValidityMask::with_allocator(count, validity_allocator),
            count,
            selection: VectorSelection::None,
            child: Some(child),
            children: Vec::new(),
            string_heap: None,
            dictionary_info: None,
            lifetime_owners: None,
        };
        // Set child count to match array_size * count
        if let Some(child_arc) = &mut vec.child {
            if let Some(child_mut) = Arc::get_mut(child_arc) {
                child_mut.set_count(array_size * count);
            }
        }
        Ok(vec)
    }

    /// Create an array vector with the given type and capacity.
    ///
    /// # Arguments
    /// * `array_type` - The `LogicalType::Array` type
    /// * `capacity` - Number of arrays to allocate space for
    pub fn try_new_array(
        array_type: LogicalType,
        capacity: usize,
        allocator: Arc<dyn Allocator>,
    ) -> Result<Self> {
        let (child_type, array_size) = match &array_type {
            LogicalType::Array(child, size) => (child.as_ref().clone(), *size),
            _ => panic!("new_array requires Array type"),
        };

        // Create child vector with capacity = array_size * capacity
        let child_capacity = array_size * capacity;
        let mut child = Vector::try_new(child_type, child_capacity, allocator.clone())?;
        let validity_allocator = child.buffer.allocator().clone();
        child.set_count(child_capacity);

        Ok(Self {
            vector_type: VectorType::Flat,
            logical_type: array_type,
            buffer: VectorBuffer::try_with_allocator(0, 0, allocator)?,
            validity: ValidityMask::with_allocator(capacity, validity_allocator),
            count: 0,
            selection: VectorSelection::None,
            child: Some(Arc::new(child)),
            children: Vec::new(),
            string_heap: None,
            dictionary_info: None,
            lifetime_owners: None,
        })
    }

    /// Create a constant vector from a Value.
    pub fn try_constant_from_value(
        logical_type: LogicalType,
        value: Value,
        count: usize,
        allocator: Arc<dyn Allocator>,
    ) -> Result<Self> {
        if matches!(value, Value::Null(_)) {
            return Self::try_constant_null(logical_type, count, allocator);
        }

        match value {
            Value::Boolean(v) => Self::try_constant(logical_type, v, count, allocator),
            Value::TinyInt(v) => Self::try_constant(logical_type, v, count, allocator),
            Value::SmallInt(v) => Self::try_constant(logical_type, v, count, allocator),
            Value::Integer(v) => Self::try_constant(logical_type, v, count, allocator),
            Value::BigInt(v) => Self::try_constant(logical_type, v, count, allocator),
            Value::HugeInt(v) => Self::try_constant(logical_type, v, count, allocator),
            Value::UTinyInt(v) => Self::try_constant(logical_type, v, count, allocator),
            Value::USmallInt(v) => Self::try_constant(logical_type, v, count, allocator),
            Value::UInteger(v) => Self::try_constant(logical_type, v, count, allocator),
            Value::UBigInt(v) => Self::try_constant(logical_type, v, count, allocator),
            Value::UHugeInt(v) => Self::try_constant(logical_type, v, count, allocator),
            Value::Uuid(v) => Self::try_constant(logical_type, v, count, allocator),
            Value::Float(v) => Self::try_constant(logical_type, v, count, allocator),
            Value::Double(v) => Self::try_constant(logical_type, v, count, allocator),
            Value::Decimal(v, precision, _scale) => {
                let width = if let LogicalType::Decimal { precision, .. } = &logical_type {
                    *precision
                } else {
                    precision
                };
                if width <= 18 {
                    let narrow =
                        i64::try_from(v).expect("Decimal value exceeds i64 range for precision");
                    Self::try_constant(logical_type, narrow, count, allocator)
                } else {
                    Self::try_constant(logical_type, v, count, allocator)
                }
            }
            Value::Varchar(v) => {
                let mut vec = Self::try_new(logical_type, 1, allocator)?;
                vec.set_value(0, &Value::Varchar(v));
                vec.vector_type = VectorType::Constant;
                vec.count = count;
                Ok(vec)
            }
            Value::Blob(v) => {
                let mut vec = Self::try_new(logical_type, 1, allocator)?;
                vec.set_value(0, &Value::Blob(v));
                vec.vector_type = VectorType::Constant;
                vec.count = count;
                Ok(vec)
            }
            Value::Date(v) => Self::try_constant(logical_type, v, count, allocator),
            Value::Time(v) => Self::try_constant(logical_type, v, count, allocator),
            Value::Timestamp(v) => Self::try_constant(logical_type, v, count, allocator),
            Value::TimestampTz(v) => Self::try_constant(logical_type, v, count, allocator),
            Value::Interval(months, days, micros) => {
                let mut vec = Self::try_new(logical_type, 1, allocator)?;
                vec.set_value(0, &Value::Interval(months, days, micros));
                vec.vector_type = VectorType::Constant;
                vec.count = count;
                Ok(vec)
            }
            Value::Array(_, _, _) | Value::List(_, _) | Value::Struct(_, _) => {
                // Nested constants use the same canonical flat representation as
                // ordinary vectors. Only the outer row is constant; its child
                // payload remains a one-row materialization that copy/gather can
                // consume through the regular nested-vector paths.
                let mut vec = Self::try_new(logical_type, 1, allocator)?;
                vec.set_value(0, &value);
                // Establish the single physical row before exposing an arbitrary
                // logical constant cardinality. Array and struct children follow
                // physical cardinality, while list children keep their payload
                // length; try_set_count encodes those invariants centrally.
                vec.try_set_count(1)?;
                vec.vector_type = VectorType::Constant;
                vec.count = count;
                Ok(vec)
            }
            // Add other types as needed
            _ => Self::try_constant_null(logical_type, count, allocator),
        }
    }

    /// Create a constant vector from a scalar runtime value.
    pub fn try_constant_scalar(
        logical_type: LogicalType,
        value: Value,
        count: usize,
        allocator: Arc<dyn Allocator>,
    ) -> Result<Self> {
        Self::try_constant_from_value(logical_type, value, count, allocator)
    }
}
