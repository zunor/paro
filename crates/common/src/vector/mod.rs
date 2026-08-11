// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Vector module - columnar data storage.
//!
//! The vector layer provides the core columnar primitives used by execution:
//! `VectorType`, `VectorBuffer`, `StringHeap`, and the shared `Vector` handle.

mod allocation_set;
mod array_vector;
mod boolean_ops;
mod comparison_ops;
mod copy;
mod definition;
mod hash_ops;
mod null_ops;
mod selection_vector;
mod string_heap;
mod validity_mask;
mod vector_access;
mod vector_buffer;
mod vector_creation;
mod vector_ops;
mod vector_type;
mod view;

#[cfg(test)]
mod tests;

/// Vectorized operations on [`Vector`] values (comparison, hash, null handling, etc.).
pub struct VectorOperations;

pub use allocation_set::AllocationSet;
pub use array_vector::{ArrayVector, VectorArrayBuffer};
pub(crate) use definition::VectorResetState;
pub use definition::{DictionaryInfo, DictionarySource, Vector};
pub use selection_vector::{
    reset_selection_materialization_count, selection_materialization_count, SelectionVector,
    VectorSelection,
};
pub use string_heap::StringHeap;
pub use validity_mask::{ValidityMask, BITS_PER_VALUE, MAX_ENTRY};
pub(crate) use vector_buffer::VectorBuffer;
pub use vector_type::VectorType;
pub use view::{
    ArrayView, DataRef, DecodedVectorOwned, DecodedVectorRef, DecodedVectorTree, SelectionRef,
    ValidityRef, VarlenView, VectorView,
};

/// Default vector size (number of rows per vector).
pub const VECTOR_SIZE: usize = 4096;
