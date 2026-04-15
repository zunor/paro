// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Vector Functions (pgvector-compatible)
//!
//!
//!
//! ## Implemented Functions
//! - `l2_distance` - Euclidean distance (L2 norm)
//! - `l1_distance` - Manhattan distance (L1 norm)
//! - `cosine_distance` - Cosine distance
//! - `inner_product` - Inner product (dot product)
//! - `vector_dims` - Get vector dimensions
//! - `vector_norm` - Get vector L2 norm

mod distance;

pub use distance::*;

use crate::ScalarFunctionSet;

/// Register all vector functions.
pub fn register_vector_functions() -> Vec<ScalarFunctionSet> {
    vec![
        get_l2_distance_functions(),
        get_l1_distance_functions(),
        get_cosine_distance_functions(),
        get_inner_product_functions(),
        get_neg_inner_product_functions(),
        get_sparse_distance_functions(),
        get_vector_dims_functions(),
        get_vector_norm_functions(),
    ]
}
