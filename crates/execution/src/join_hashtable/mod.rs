// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Hash table primitives used by join operators.

mod build_store;
pub(crate) mod hash_kernel;
mod integer_index;
mod reduction_extrema;

pub mod ht_entry;
pub mod scan_structure;
pub mod table;

pub(crate) use reduction_extrema::GroupedReductionExtrema;
pub use table::{FullOuterScanState, JoinHashTable, JoinHashTableConfig};
