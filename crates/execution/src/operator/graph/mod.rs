// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Graph query physical operators.
//!
//! Implements the physical execution of SQL/PGQ graph pattern matching:
//! - `PhysicalGraphScan`: Scans a vertex table, producing (local_vertex_id, rowid) tuples
//! - `PhysicalGraphExpand`: Expands from source vertices along edges via CSR adjacency
//! - `PhysicalGraphProject`: Late materialization — reads actual column values from tables using rowids
//! - `PhysicalGraphShortestPath`: BFS shortest path with lane-parallel bitset optimization

mod graph_cardinality;
pub mod graph_expand;
mod graph_output_buffer;
pub mod graph_path;
pub mod graph_project;
pub mod graph_scan;
pub mod graph_shortest_path;
pub mod spillable_frontier;
pub mod spillable_parent_arrays;
