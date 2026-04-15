// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Shared graph cardinality estimation helpers for EXPLAIN and planning diagnostics.

use paro_catalog::entry::VertexTableInfo;
use paro_planner::expression::Expression;
use paro_planner::operator::ExpandDirection;

pub(crate) fn estimate_scan_cardinality(
    filter: Option<&Expression>,
    vertex_info: &VertexTableInfo,
) -> usize {
    default_scan_rows(filter, vertex_info)
}

pub(crate) fn estimate_expand_cardinality(
    child_rows: usize,
    direction: ExpandDirection,
    min_hops: u64,
    max_hops: u64,
) -> usize {
    let _ = direction;
    fallback_expand_rows(child_rows, min_hops, max_hops)
}

fn estimate_hop_multiplier(min_hops: u64, max_hops: u64) -> f64 {
    if min_hops == 1 && max_hops == 1 {
        return 1.0;
    }
    let capped_max = if max_hops == u64::MAX {
        4
    } else {
        max_hops.min(4)
    };
    capped_max.max(min_hops.max(1)) as f64
}

fn default_scan_rows(filter: Option<&Expression>, vertex_info: &VertexTableInfo) -> usize {
    if filter.is_none() {
        1000
    } else if !vertex_info.key_column_ids.is_empty() {
        1
    } else {
        100
    }
}

fn fallback_expand_rows(child_rows: usize, min_hops: u64, max_hops: u64) -> usize {
    let branch_factor = 4usize;
    let hop_multiplier = estimate_hop_multiplier(min_hops, max_hops).ceil() as usize;
    child_rows
        .max(1)
        .saturating_mul(branch_factor)
        .saturating_mul(hop_multiplier.max(1))
}
