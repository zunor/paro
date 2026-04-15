// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Scan operators.

pub mod adaptive_scan;
pub mod column_data_scan;
pub mod column_pruning;
pub mod dummy_scan;
pub mod expression_scan;
pub mod late_materialize;
pub mod memory_budget;
pub mod rowset_scan;
pub mod table_function;

pub mod fulltext_scan;
pub mod sparse_vector_scan;
pub mod vector_scan;
