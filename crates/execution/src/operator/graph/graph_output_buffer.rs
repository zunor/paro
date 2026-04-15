// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::mem;

use paro_common::vector::VECTOR_SIZE;

use super::graph_path::MaterializedPath;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EmitAction {
    Continue,
    Yield,
}

#[derive(Debug, Default)]
pub struct GraphPathOutputBuffer {
    capacity_rows: usize,
    track_paths: bool,
    rows: Vec<Vec<u64>>,
    path_rows: Vec<MaterializedPath>,
}

impl GraphPathOutputBuffer {
    pub fn new(track_paths: bool) -> Self {
        Self {
            capacity_rows: VECTOR_SIZE,
            track_paths,
            rows: Vec::with_capacity(VECTOR_SIZE),
            path_rows: Vec::with_capacity(if track_paths { VECTOR_SIZE } else { 0 }),
        }
    }

    pub fn clear(&mut self) {
        self.rows.clear();
        self.path_rows.clear();
    }

    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    pub fn is_full(&self) -> bool {
        self.rows.len() >= self.capacity_rows
    }

    pub fn push_row(&mut self, row: Vec<u64>, path: Option<MaterializedPath>) -> EmitAction {
        if self.track_paths {
            self.path_rows
                .push(path.expect("Graph path output requires a matching path row"));
        } else {
            debug_assert!(path.is_none());
        }
        self.rows.push(row);
        if self.is_full() {
            EmitAction::Yield
        } else {
            EmitAction::Continue
        }
    }

    pub fn take(&mut self) -> (Vec<Vec<u64>>, Vec<MaterializedPath>) {
        (mem::take(&mut self.rows), mem::take(&mut self.path_rows))
    }
}
