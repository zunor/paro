use super::{
    DictionaryInfo, DictionarySource, SelectionVector, StringHeap, ValidityMask, Vector,
    VectorBuffer, VectorType,
};
use crate::allocator::{default_allocator, Allocator};
use crate::runtime_value::Value;
use crate::types::{InlineString, LogicalType};
use std::sync::Arc;

impl Vector {
    /// Create a flat vector from i64 values with allocator.
    pub fn from_i64_with_allocator(values: &[i64], allocator: Arc<dyn Allocator>) -> Self {
        let mut vec =
            Self::with_capacity_and_allocator(LogicalType::BigInt, values.len(), allocator);
        vec.count = values.len();
        vec.validity = ValidityMask::with_allocator(values.len(), vec.buffer.allocator().clone());

        // SAFETY: We know the type is i64
        unsafe {
            let ptr = vec.buffer.data() as *mut i64;
            std::ptr::copy_nonoverlapping(values.as_ptr(), ptr, values.len());
        }
        vec
    }

    /// Create a flat vector from i32 values with allocator.
    pub fn from_i32_with_allocator(values: &[i32], allocator: Arc<dyn Allocator>) -> Self {
        let mut vec =
            Self::with_capacity_and_allocator(LogicalType::Integer, values.len(), allocator);
        vec.count = values.len();
        vec.validity = ValidityMask::with_allocator(values.len(), vec.buffer.allocator().clone());

        // SAFETY: We know the type is i32
        unsafe {
            let ptr = vec.buffer.data() as *mut i32;
            std::ptr::copy_nonoverlapping(values.as_ptr(), ptr, values.len());
        }
        vec
    }

    /// Create a flat vector from f64 values with allocator.
    pub fn from_f64_with_allocator(values: &[f64], allocator: Arc<dyn Allocator>) -> Self {
        let mut vec =
            Self::with_capacity_and_allocator(LogicalType::Double, values.len(), allocator);
        vec.count = values.len();
        vec.validity = ValidityMask::with_allocator(values.len(), vec.buffer.allocator().clone());

        // SAFETY: We know the type is f64
        unsafe {
            let ptr = vec.buffer.data() as *mut f64;
            std::ptr::copy_nonoverlapping(values.as_ptr(), ptr, values.len());
        }
        vec
    }

    /// Create a flat vector from f32 values with allocator.
    pub fn from_f32_with_allocator(values: &[f32], allocator: Arc<dyn Allocator>) -> Self {
        let mut vec =
            Self::with_capacity_and_allocator(LogicalType::Float, values.len(), allocator);
        vec.count = values.len();
        vec.validity = ValidityMask::with_allocator(values.len(), vec.buffer.allocator().clone());

        // SAFETY: We know the type is f32
        unsafe {
            let ptr = vec.buffer.data() as *mut f32;
            std::ptr::copy_nonoverlapping(values.as_ptr(), ptr, values.len());
        }
        vec
    }

    /// Create a flat vector from bool values with allocator.
    pub fn from_bool_with_allocator(values: &[bool], allocator: Arc<dyn Allocator>) -> Self {
        let mut vec =
            Self::with_capacity_and_allocator(LogicalType::Boolean, values.len(), allocator);
        vec.count = values.len();
        vec.validity = ValidityMask::with_allocator(values.len(), vec.buffer.allocator().clone());

        // SAFETY: We know the type is bool
        unsafe {
            let ptr = vec.buffer.data() as *mut bool;
            std::ptr::copy_nonoverlapping(values.as_ptr(), ptr, values.len());
        }
        vec
    }

    /// Create a flat vector from nullable bool values.
    pub fn from_nullable_bools(values: &[Option<bool>]) -> Self {
        let allocator = Arc::new(default_allocator());
        let mut vec =
            Self::with_capacity_and_allocator(LogicalType::Boolean, values.len(), allocator);
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
        vec
    }

    /// Create a flat vector from nullable u64 values (mapped to BigInt).
    pub fn from_nullable_u64(values: &[Option<u64>]) -> Self {
        let allocator = Arc::new(default_allocator());
        let mut vec =
            Self::with_capacity_and_allocator(LogicalType::BigInt, values.len(), allocator);
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
        vec
    }

    /// Create a flat vector from nullable string values.
    pub fn from_nullable_strings(values: &[Option<&str>]) -> Self {
        let allocator = Arc::new(default_allocator());
        let mut vec = Self::with_capacity_and_allocator(
            LogicalType::Varchar,
            values.len(),
            allocator.clone(),
        );
        vec.count = values.len();
        vec.validity = ValidityMask::with_allocator(values.len(), vec.buffer.allocator().clone());

        // Create heap for string storage
        let mut heap = StringHeap::with_allocator(4096, allocator.clone());

        let buffer = VectorBuffer::with_allocator(
            std::mem::size_of::<InlineString>(),
            values.len(),
            allocator,
        );

        // SAFETY: We allocated space for InlineString array
        unsafe {
            let entries = buffer.data() as *mut InlineString;
            for (i, s) in values.iter().enumerate() {
                match s {
                    Some(str_val) => {
                        // add_string handles both short (inlined) and long (heap) strings
                        *entries.add(i) = heap.add_string(str_val);
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
        vec
    }

    /// Create a flat vector from strings with allocator.
    pub fn from_strings_with_allocator(values: &[&str], allocator: Arc<dyn Allocator>) -> Self {
        let mut vec = Self::with_capacity_and_allocator(
            LogicalType::Varchar,
            values.len(),
            allocator.clone(),
        );
        vec.count = values.len();
        vec.validity = ValidityMask::with_allocator(values.len(), vec.buffer.allocator().clone());

        // Create heap for string storage
        let mut heap = StringHeap::with_allocator(4096, allocator.clone());

        let buffer = VectorBuffer::with_allocator(
            std::mem::size_of::<InlineString>(),
            values.len(),
            allocator,
        );

        // SAFETY: We allocated space for InlineString array
        unsafe {
            let entries = buffer.data() as *mut InlineString;
            for (i, s) in values.iter().enumerate() {
                // add_string handles both short (inlined) and long (heap) strings
                *entries.add(i) = heap.add_string(s);
            }
        }

        vec.buffer = buffer;
        // Only store heap if it has allocations
        if !heap.is_empty() {
            vec.string_heap = Some(Arc::new(heap));
        }
        vec
    }

    /// Create an embedding vector with allocator.
    pub fn from_embeddings_with_allocator(
        embeddings: &[Vec<f32>],
        dimensions: usize,
        allocator: Arc<dyn Allocator>,
    ) -> Self {
        let count = embeddings.len();

        // Flatten all embeddings into a single child vector
        let flattened: Vec<f32> = embeddings.iter().flatten().copied().collect();
        let child = Self::from_f32_with_allocator(&flattened, allocator.clone());

        Self {
            vector_type: VectorType::Flat,
            logical_type: LogicalType::Array(Box::new(LogicalType::Float), dimensions),
            buffer: VectorBuffer::with_allocator(0, 0, allocator),
            validity: ValidityMask::with_allocator(count, child.buffer.allocator().clone()),
            count,
            sel_vector: None,
            child: Some(Arc::new(child)),
            children: Vec::new(),
            string_heap: None,
            dictionary_info: None,
        }
    }

    /// Create a constant vector with a single value and allocator.
    pub fn constant_with_allocator<T: Copy>(
        logical_type: LogicalType,
        value: T,
        count: usize,
        allocator: Arc<dyn Allocator>,
    ) -> Self {
        let element_size = std::mem::size_of::<T>();
        let buffer = VectorBuffer::with_allocator(element_size, 1, allocator);
        let validity_allocator = buffer.allocator().clone();

        // SAFETY: We just allocated space for one T
        unsafe {
            let ptr = buffer.data() as *mut T;
            *ptr = value;
        }

        Self {
            vector_type: VectorType::Constant,
            buffer,
            validity: ValidityMask::with_allocator(1, validity_allocator),
            count,
            logical_type,
            sel_vector: None,
            child: None,
            children: Vec::new(),
            string_heap: None,
            dictionary_info: None,
        }
    }

    /// Create a constant null vector with allocator.
    pub fn constant_null_with_allocator(
        logical_type: LogicalType,
        count: usize,
        allocator: Arc<dyn Allocator>,
    ) -> Self {
        let buffer = VectorBuffer::with_allocator(0, 0, allocator);
        let validity_allocator = buffer.allocator().clone();
        let mut vec = Self {
            vector_type: VectorType::Constant,
            buffer,
            validity: ValidityMask::with_allocator(1, validity_allocator),
            count,
            logical_type,
            sel_vector: None,
            child: None,
            children: Vec::new(),
            string_heap: None,
            dictionary_info: None,
        };
        vec.validity.set_null(0);
        vec
    }

    /// Create a sequence vector with allocator.
    pub fn sequence_with_allocator(
        start: i64,
        increment: i64,
        count: usize,
        allocator: Arc<dyn Allocator>,
    ) -> Self {
        let buffer = VectorBuffer::with_allocator(std::mem::size_of::<i64>(), 2, allocator);
        let validity_allocator = buffer.allocator().clone();

        // SAFETY: We allocated space for 2 i64s
        unsafe {
            let ptr = buffer.data() as *mut i64;
            *ptr = start;
            *ptr.add(1) = increment;
        }

        Self {
            vector_type: VectorType::Sequence,
            buffer,
            validity: ValidityMask::with_allocator(count, validity_allocator),
            count,
            logical_type: LogicalType::BigInt,
            sel_vector: None,
            child: None,
            children: Vec::new(),
            string_heap: None,
            dictionary_info: None,
        }
    }

    fn dictionary_with_info<S>(
        child: Arc<Vector>,
        selection: S,
        dictionary_info: DictionaryInfo,
    ) -> Self
    where
        S: Into<SelectionVector>,
    {
        let sel = selection.into();
        let (base_child, combined_sel) = if child.vector_type == VectorType::Dictionary {
            let child_sel = child
                .sel_vector
                .as_ref()
                .expect("Dictionary vector missing selection vector");
            let base_child = child
                .child
                .as_ref()
                .expect("Dictionary vector missing child")
                .clone();
            let merged_sel = child_sel.slice(&sel, sel.len());
            (base_child, merged_sel)
        } else {
            (child, sel)
        };

        let allocator = base_child.buffer.allocator().clone();
        let count = combined_sel.len();
        let validity_allocator = base_child.buffer.allocator().clone();
        Self {
            vector_type: VectorType::Dictionary,
            logical_type: base_child.logical_type.clone(),
            buffer: VectorBuffer::with_allocator(0, 0, allocator),
            validity: ValidityMask::with_allocator(count, validity_allocator),
            count,
            sel_vector: Some(combined_sel),
            child: Some(base_child),
            children: Vec::new(),
            string_heap: None,
            dictionary_info: Some(dictionary_info),
        }
    }

    /// Create a canonicalized dictionary vector that represents a generic selection overlay.
    pub fn dictionary<S>(child: Arc<Vector>, selection: S) -> Self
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
        Self::dictionary_with_info(
            child,
            selection,
            DictionaryInfo {
                unique_len,
                provenance_id: None,
                source: DictionarySource::GenericSelection,
            },
        )
    }

    /// Create a dictionary vector with explicit provenance metadata.
    pub fn with_dictionary<S>(
        child: Arc<Vector>,
        selection: S,
        dictionary_info: DictionaryInfo,
    ) -> Self
    where
        S: Into<SelectionVector>,
    {
        Self::dictionary_with_info(child, selection, dictionary_info)
    }

    /// Create an array vector from flattened data.
    pub fn from_array(
        element_type: LogicalType,
        child: Arc<Vector>,
        count: usize,
        array_size: usize,
    ) -> Self {
        let allocator = child.buffer.allocator().clone();
        let validity_allocator = child.buffer.allocator().clone();
        let mut vec = Self {
            vector_type: VectorType::Flat,
            logical_type: LogicalType::Array(Box::new(element_type), array_size),
            buffer: VectorBuffer::with_allocator(0, 0, allocator),
            validity: ValidityMask::with_allocator(count, validity_allocator),
            count,
            sel_vector: None,
            child: Some(child),
            children: Vec::new(),
            string_heap: None,
            dictionary_info: None,
        };
        // Set child count to match array_size * count
        if let Some(child_arc) = &mut vec.child {
            if let Some(child_mut) = Arc::get_mut(child_arc) {
                child_mut.set_count(array_size * count);
            }
        }
        vec
    }

    /// Create an array vector with the given type and capacity.
    ///
    /// # Arguments
    /// * `array_type` - The `LogicalType::Array` type
    /// * `capacity` - Number of arrays to allocate space for
    pub fn new_array(array_type: LogicalType, capacity: usize) -> Self {
        Self::new_array_with_allocator(array_type, capacity, Arc::new(default_allocator()))
    }

    /// Create an array vector with the given type, capacity, and allocator.
    pub fn new_array_with_allocator(
        array_type: LogicalType,
        capacity: usize,
        allocator: Arc<dyn Allocator>,
    ) -> Self {
        let (child_type, array_size) = match &array_type {
            LogicalType::Array(child, size) => (child.as_ref().clone(), *size),
            _ => panic!("new_array requires Array type"),
        };

        // Create child vector with capacity = array_size * capacity
        let child_capacity = array_size * capacity;
        let mut child =
            Vector::with_capacity_and_allocator(child_type, child_capacity, allocator.clone());
        let validity_allocator = child.buffer.allocator().clone();
        child.set_count(child_capacity);

        Self {
            vector_type: VectorType::Flat,
            logical_type: array_type,
            buffer: VectorBuffer::with_allocator(0, 0, allocator),
            validity: ValidityMask::with_allocator(capacity, validity_allocator),
            count: 0,
            sel_vector: None,
            child: Some(Arc::new(child)),
            children: Vec::new(),
            string_heap: None,
            dictionary_info: None,
        }
    }

    /// Create a constant vector from a Value.
    pub fn constant_from_value(logical_type: LogicalType, value: Value, count: usize) -> Self {
        let allocator = Arc::new(default_allocator());

        if matches!(value, Value::Null(_)) {
            return Self::constant_null_with_allocator(logical_type, count, allocator);
        }

        match value {
            Value::Boolean(v) => Self::constant_with_allocator(logical_type, v, count, allocator),
            Value::TinyInt(v) => Self::constant_with_allocator(logical_type, v, count, allocator),
            Value::SmallInt(v) => Self::constant_with_allocator(logical_type, v, count, allocator),
            Value::Integer(v) => Self::constant_with_allocator(logical_type, v, count, allocator),
            Value::BigInt(v) => Self::constant_with_allocator(logical_type, v, count, allocator),
            Value::HugeInt(v) => Self::constant_with_allocator(logical_type, v, count, allocator),
            Value::UTinyInt(v) => Self::constant_with_allocator(logical_type, v, count, allocator),
            Value::USmallInt(v) => Self::constant_with_allocator(logical_type, v, count, allocator),
            Value::UInteger(v) => Self::constant_with_allocator(logical_type, v, count, allocator),
            Value::UBigInt(v) => Self::constant_with_allocator(logical_type, v, count, allocator),
            Value::UHugeInt(v) => Self::constant_with_allocator(logical_type, v, count, allocator),
            Value::Uuid(v) => Self::constant_with_allocator(logical_type, v, count, allocator),
            Value::Float(v) => Self::constant_with_allocator(logical_type, v, count, allocator),
            Value::Double(v) => Self::constant_with_allocator(logical_type, v, count, allocator),
            Value::Decimal(v, precision, _scale) => {
                let width = if let LogicalType::Decimal { precision, .. } = &logical_type {
                    *precision
                } else {
                    precision
                };
                if width <= 18 {
                    let narrow =
                        i64::try_from(v).expect("Decimal value exceeds i64 range for precision");
                    Self::constant_with_allocator(logical_type, narrow, count, allocator)
                } else {
                    Self::constant_with_allocator(logical_type, v, count, allocator)
                }
            }
            Value::Varchar(v) => {
                let mut vec = Self::with_capacity_and_allocator(logical_type, 1, allocator);
                vec.set_value(0, &Value::Varchar(v));
                vec.vector_type = VectorType::Constant;
                vec.count = count;
                vec
            }
            Value::Blob(v) => {
                let mut vec = Self::with_capacity_and_allocator(logical_type, 1, allocator);
                vec.set_value(0, &Value::Blob(v));
                vec.vector_type = VectorType::Constant;
                vec.count = count;
                vec
            }
            Value::Date(v) => Self::constant_with_allocator(logical_type, v, count, allocator),
            Value::Time(v) => Self::constant_with_allocator(logical_type, v, count, allocator),
            Value::Timestamp(v) => Self::constant_with_allocator(logical_type, v, count, allocator),
            Value::TimestampTz(v) => {
                Self::constant_with_allocator(logical_type, v, count, allocator)
            }
            Value::Interval(months, days, micros) => {
                let mut vec = Self::with_capacity_and_allocator(logical_type, 1, allocator);
                vec.set_value(0, &Value::Interval(months, days, micros));
                vec.vector_type = VectorType::Constant;
                vec.count = count;
                vec
            }
            Value::Array(_, _, _) | Value::List(_, _) | Value::Struct(_, _) => {
                // For Array, List, and Struct, we use reference_value to set up child vectors
                let mut vec = Self::with_capacity_and_allocator(logical_type, 1, allocator);
                vec.reference_value(&value);
                vec.count = count;
                vec
            }
            // Add other types as needed
            _ => Self::constant_null_with_allocator(logical_type, count, allocator),
        }
    }
}
