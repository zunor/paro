// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Array Vector implementation.
//!
//! ## Description
//! Array is a fixed-size array type (e.g., `FLOAT[1536]` for AI embeddings).
//! Unlike List (variable-length), Array has a fixed size known at compile time.
//!
//! ## Storage Layout
//! - Array data is stored in a child vector
//! - Child vector size = array_size * count
//! - Access element j of array i: child[i * array_size + j]
//!
//! ## NULL Handling
//! - Entire array can be NULL (parent validity mask)
//! - Individual elements can be NULL (child vector validity mask)

use std::sync::Arc;

use crate::allocator::{default_allocator, Allocator};
use crate::types::LogicalType;

use super::{Vector, VectorType};

/// `VectorArrayBuffer` holds the child vector for Array type.
///
/// ## Fields
/// - `child`: Child vector containing all array elements (flattened)
/// - `array_size`: Fixed size of each array
/// - `size`: Number of arrays currently stored
#[derive(Debug, Clone)]
pub struct VectorArrayBuffer {
    /// Child vector containing flattened array elements.
    /// Total elements = array_size * size
    child: Vector,
    /// Fixed size of each array (e.g., 1536 for embeddings)
    array_size: usize,
    /// Number of arrays currently stored
    size: usize,
}

impl VectorArrayBuffer {
    /// Create a new `VectorArrayBuffer` with the given array type and initial capacity.
    ///
    /// # Arguments
    /// * `array_type` - The `LogicalType::Array` type
    /// * `initial_capacity` - Initial number of arrays to allocate space for
    pub fn new(array_type: &LogicalType, initial_capacity: usize) -> Self {
        let (child_type, array_size) = match array_type {
            LogicalType::Array(child, size) => (child.as_ref().clone(), *size),
            _ => panic!("VectorArrayBuffer requires Array type"),
        };

        assert!(array_size > 0, "Array size must be greater than 0");

        // Create child vector with capacity = array_size * initial_capacity
        let child_capacity = array_size * initial_capacity;
        let child = Vector::try_new(child_type, child_capacity, Arc::new(default_allocator()))
            .expect("array child allocation failed");

        Self {
            child,
            array_size,
            size: initial_capacity,
        }
    }

    /// Create a new VectorArrayBuffer with a custom allocator.
    pub fn with_allocator(
        array_type: &LogicalType,
        initial_capacity: usize,
        allocator: Arc<dyn Allocator>,
    ) -> Self {
        let (child_type, array_size) = match array_type {
            LogicalType::Array(child, size) => (child.as_ref().clone(), *size),
            _ => panic!("VectorArrayBuffer requires Array type"),
        };

        assert!(array_size > 0, "Array size must be greater than 0");

        // Create child vector with capacity = array_size * initial_capacity
        let child_capacity = array_size * initial_capacity;
        let child = Vector::try_new(child_type, child_capacity, allocator)
            .expect("array child allocation failed");

        Self {
            child,
            array_size,
            size: initial_capacity,
        }
    }

    /// Create a VectorArrayBuffer from an existing child vector.
    ///
    /// # Arguments
    /// * `child` - The child vector containing flattened array elements
    /// * `array_size` - Fixed size of each array
    /// * `initial_capacity` - Number of arrays
    pub fn from_child(child: Vector, array_size: usize, initial_capacity: usize) -> Self {
        assert!(array_size > 0, "Array size must be greater than 0");

        Self {
            child,
            array_size,
            size: initial_capacity,
        }
    }

    /// Get a reference to the child vector.
    #[inline]
    pub fn get_child(&self) -> &Vector {
        &self.child
    }

    /// Get a mutable reference to the child vector.
    #[inline]
    pub fn get_child_mut(&mut self) -> &mut Vector {
        &mut self.child
    }

    /// Get the fixed array size.
    #[inline]
    pub fn get_array_size(&self) -> usize {
        self.array_size
    }

    /// Get the total size of the child vector (`array_size * size`).
    #[inline]
    pub fn get_child_size(&self) -> usize {
        self.size * self.array_size
    }

    /// Get the number of arrays stored.
    #[inline]
    pub fn get_size(&self) -> usize {
        self.size
    }

    /// Set the number of arrays stored.
    #[inline]
    pub fn set_size(&mut self, size: usize) {
        self.size = size;
    }
}

/// `ArrayVector` helper functions.
pub struct ArrayVector;

impl ArrayVector {
    /// Get a reference to the underlying child vector of an array.
    ///
    /// # Panics
    /// Panics if the vector is not an Array type.
    pub fn get_entry(vector: &Vector) -> &Vector {
        assert!(
            matches!(vector.logical_type(), LogicalType::Array(_, _)),
            "ArrayVector::get_entry requires Array type"
        );

        match vector.vector_type() {
            VectorType::Dictionary => {
                // For dictionary vectors, recurse into the child
                let child = vector.child().expect("Dictionary missing child");
                ArrayVector::get_entry(child)
            }
            VectorType::Flat | VectorType::Constant => {
                // For flat/constant vectors, return the child vector
                vector.child().expect("Array vector missing child")
            }
            VectorType::Sequence => {
                panic!("Sequence vectors cannot be Array type")
            }
        }
    }

    /// Get a mutable reference to the underlying child vector of an array.
    ///
    /// # Panics
    /// Panics if the vector is not an Array type.
    pub fn get_entry_mut(vector: &mut Vector) -> &mut Vector {
        assert!(
            matches!(vector.logical_type(), LogicalType::Array(_, _)),
            "ArrayVector::get_entry_mut requires Array type"
        );

        // We need to get mutable access to the child
        // This requires the child to be exclusively owned
        vector.make_exclusive();

        // Get the child Arc and try to get mutable access
        let child = vector.child_mut().expect("Array vector missing child");

        Arc::make_mut(child)
    }

    /// Get the total size of the underlying child vector.
    ///
    /// This is `array_size * count`.
    pub fn get_total_size(vector: &Vector) -> usize {
        let array_size = match vector.logical_type() {
            LogicalType::Array(_, size) => *size,
            _ => panic!("ArrayVector::get_total_size requires Array type"),
        };

        array_size * vector.len()
    }

    /// Get the array size from the vector's type.
    pub fn get_array_size(vector: &Vector) -> usize {
        match vector.logical_type() {
            LogicalType::Array(_, size) => *size,
            _ => panic!("ArrayVector::get_array_size requires Array type"),
        }
    }

    /// Get the child type from the vector's type.
    pub fn get_child_type(vector: &Vector) -> &LogicalType {
        match vector.logical_type() {
            LogicalType::Array(child, _) => child.as_ref(),
            _ => panic!("ArrayVector::get_child_type requires Array type"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_vector_array_buffer_new() {
        let array_type = LogicalType::Array(Box::new(LogicalType::Float), 3);
        let buffer = VectorArrayBuffer::new(&array_type, 10);

        assert_eq!(buffer.get_array_size(), 3);
        assert_eq!(buffer.get_size(), 10);
        assert_eq!(buffer.get_child_size(), 30);
    }

    #[test]
    fn test_vector_array_buffer_from_child() {
        let child = crate::test_utils::test_f32_vector(&[1.0, 2.0, 3.0, 4.0, 5.0, 6.0]);
        let buffer = VectorArrayBuffer::from_child(child, 3, 2);

        assert_eq!(buffer.get_array_size(), 3);
        assert_eq!(buffer.get_size(), 2);
        assert_eq!(buffer.get_child_size(), 6);
    }

    #[test]
    fn test_array_vector_get_entry() {
        // Create an array vector with 2 arrays of 3 floats each
        let child = crate::test_utils::test_f32_vector(&[1.0, 2.0, 3.0, 4.0, 5.0, 6.0]);
        let array_vec =
            crate::test_utils::test_array_vector(LogicalType::Float, Arc::new(child), 2, 3);

        let entry = ArrayVector::get_entry(&array_vec);
        assert_eq!(entry.len(), 6);
    }

    #[test]
    fn test_array_vector_get_total_size() {
        let child = crate::test_utils::test_f32_vector(&[1.0, 2.0, 3.0, 4.0, 5.0, 6.0]);
        let mut array_vec =
            crate::test_utils::test_array_vector(LogicalType::Float, Arc::new(child), 2, 3);
        array_vec.set_count(2);

        let total_size = ArrayVector::get_total_size(&array_vec);
        assert_eq!(total_size, 6); // 2 arrays * 3 elements
    }

    #[test]
    fn test_array_vector_get_array_size() {
        let child = crate::test_utils::test_f32_vector(&[1.0, 2.0, 3.0]);
        let array_vec =
            crate::test_utils::test_array_vector(LogicalType::Float, Arc::new(child), 1, 3);

        let array_size = ArrayVector::get_array_size(&array_vec);
        assert_eq!(array_size, 3);
    }

    #[test]
    fn test_array_vector_get_child_type() {
        let child = crate::test_utils::test_f32_vector(&[1.0, 2.0, 3.0]);
        let array_vec =
            crate::test_utils::test_array_vector(LogicalType::Float, Arc::new(child), 1, 3);

        let child_type = ArrayVector::get_child_type(&array_vec);
        assert_eq!(*child_type, LogicalType::Float);
    }

    #[test]
    fn test_array_vector_set_and_get_value() {
        use crate::runtime_value::Value;

        // Create an array vector with 2 arrays of 3 floats each
        let array_type = LogicalType::Array(Box::new(LogicalType::Float), 3);
        let mut array_vec = crate::test_utils::test_new_array(array_type, 2);
        array_vec.set_count(2);

        // Set values for first array [1.0, 2.0, 3.0]
        let val1 = Value::Array(
            vec![Value::Float(1.0), Value::Float(2.0), Value::Float(3.0)],
            LogicalType::Float,
            3,
        );
        array_vec.set_value(0, &val1);

        // Set values for second array [4.0, 5.0, 6.0]
        let val2 = Value::Array(
            vec![Value::Float(4.0), Value::Float(5.0), Value::Float(6.0)],
            LogicalType::Float,
            3,
        );
        array_vec.set_value(1, &val2);

        // Get and verify values
        let retrieved1 = array_vec.get_value(0);
        let retrieved2 = array_vec.get_value(1);

        match retrieved1 {
            Value::Array(children, _, size) => {
                assert_eq!(size, 3);
                assert_eq!(children.len(), 3);
                assert_eq!(children[0], Value::Float(1.0));
                assert_eq!(children[1], Value::Float(2.0));
                assert_eq!(children[2], Value::Float(3.0));
            }
            _ => panic!("Expected Array value"),
        }

        match retrieved2 {
            Value::Array(children, _, size) => {
                assert_eq!(size, 3);
                assert_eq!(children.len(), 3);
                assert_eq!(children[0], Value::Float(4.0));
                assert_eq!(children[1], Value::Float(5.0));
                assert_eq!(children[2], Value::Float(6.0));
            }
            _ => panic!("Expected Array value"),
        }
    }

    #[test]
    fn test_array_vector_with_nulls() {
        use crate::runtime_value::Value;

        // Create an array vector with 2 arrays of 2 integers each
        let array_type = LogicalType::Array(Box::new(LogicalType::Integer), 2);
        let mut array_vec = crate::test_utils::test_new_array(array_type, 2);
        array_vec.set_count(2);

        // Set first array [1, 2]
        let val1 = Value::Array(
            vec![Value::Integer(1), Value::Integer(2)],
            LogicalType::Integer,
            2,
        );
        array_vec.set_value(0, &val1);

        // Set second array as null
        array_vec.set_null(1, true);

        // Verify first array is valid
        assert!(!array_vec.is_null(0));

        // Verify second array is null
        assert!(array_vec.is_null(1));

        // Get first array value
        let retrieved = array_vec.get_value(0);
        match retrieved {
            Value::Array(children, _, _) => {
                assert_eq!(children[0], Value::Integer(1));
                assert_eq!(children[1], Value::Integer(2));
            }
            _ => panic!("Expected Array value"),
        }

        // Get second array value (should be null)
        let null_val = array_vec.get_value(1);
        assert!(null_val.is_null());
    }

    #[test]
    fn test_array_vector_flatten() {
        // Create an array vector
        let child = crate::test_utils::test_f32_vector(&[1.0, 2.0, 3.0, 4.0, 5.0, 6.0]);
        let mut array_vec =
            crate::test_utils::test_array_vector(LogicalType::Float, Arc::new(child), 2, 3);
        array_vec.set_count(2);

        // Flatten the vector
        array_vec.flatten();

        // Verify the child is still accessible
        let entry = ArrayVector::get_entry(&array_vec);
        assert_eq!(entry.len(), 6);
    }

    #[test]
    fn test_array_vector_recursive_unified_format() {
        // Create an array vector
        let child = crate::test_utils::test_i32_vector(&[10, 20, 30, 40, 50, 60]);
        let mut array_vec =
            crate::test_utils::test_array_vector(LogicalType::Integer, Arc::new(child), 2, 3);
        array_vec.set_count(2);

        // Get the decoded tree view
        let format = array_vec.decode_tree(2);

        // Verify the format
        assert_eq!(format.children.len(), 1);
        assert!(matches!(format.logical_type, LogicalType::Array(_, 3)));
    }
}
