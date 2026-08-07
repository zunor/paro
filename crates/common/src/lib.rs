// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! # Paro Common
//!
//! Core types, error handling, and data structures for the Paro database.
//!
//! ## Modules
//! - `config`: Configuration system (layered config loading)
//! - `logging`: Logging system (tracing-based)
//! - `error`: Error types and result aliases
//! - `types`: SQL type definitions (logical / physical / nested)
//! - `runtime_value`: SQL runtime values (`Value`, etc.)
//! - `cast_rules`: type cast compatibility rules
//! - `expression_type` / `filter_propagate`: planner / stats enums
//! - `vector`: Columnar vectors and vectorized ops
//! - `distance`: SIMD distance primitives on `f32` slices
//! - `chunk`: Collection of vectors
//! - `allocator`: Memory allocation interface
//! - `collections`: Bitset, bloom filter, HyperLogLog, etc.
//!
//! ## Note
//! Execution-time row storage lives in `paro_storage::row`.
//!
//! There are **no** crate-root `pub use` re-exports; import via submodules (e.g. `paro_common::error::Result`).

pub mod allocator;
pub mod cast_rules;
pub mod checkpoint;
pub mod chunk;
pub mod collections;
pub mod config;
pub mod ddl;
pub mod distance;
pub mod durability;
pub mod effect;
pub mod error;
pub mod expression_type;
pub mod filter_propagate;
pub mod hash;
pub mod identity;
pub mod journal;
pub mod logging;
pub mod memory;
pub mod runtime_value;
pub mod sort_key;
#[cfg(any(test, feature = "test-support"))]
pub mod test_utils;
pub mod typed_parameters;
pub mod types;
pub mod vector;
pub mod version;
