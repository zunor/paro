// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Relative fixed-edge traversal work, not calibrated elapsed time. The carrier
//! has two root identity columns and three columns per expansion. Charge each
//! adjacency visit plus each emitted carrier slot; a closing edge still visits
//! adjacency even when few pairs survive. This kernel is shared by greedy and
//! subset search, and does not encode workload-specific degree thresholds.

pub(crate) fn scan_work(rows: f64) -> f64 {
    rows * 2.0
}

pub(crate) fn expansion_work(input: f64, degree: f64, output: f64, edges: usize) -> f64 {
    input * degree + output * (2 + 3 * edges) as f64
}
