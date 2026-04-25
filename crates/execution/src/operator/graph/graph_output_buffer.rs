// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::mem::size_of;

use paro_common::error::Result;
use paro_common::memory::MemoryAccountingContext;
use paro_common::vector::VECTOR_SIZE;

use crate::memory_runtime::{AccountedBuffer, RetainedMemoryHandle};

use super::graph_path::{MaterializedPath, PathElementRef};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EmitAction {
    Continue,
    Yield,
}

#[derive(Debug)]
pub struct GraphPathOutputBuffer {
    capacity_rows: usize,
    track_paths: bool,
    memory: MemoryAccountingContext,
    rows: AccountedBuffer<Vec<u64>>,
    row_payloads: AccountedBuffer<RetainedMemoryHandle>,
    path_rows: AccountedBuffer<MaterializedPath>,
    path_payloads: AccountedBuffer<RetainedMemoryHandle>,
}

impl GraphPathOutputBuffer {
    pub fn new(track_paths: bool, memory: MemoryAccountingContext) -> Result<Self> {
        Ok(Self {
            capacity_rows: VECTOR_SIZE,
            track_paths,
            memory: memory.clone(),
            rows: AccountedBuffer::with_capacity(memory.clone(), VECTOR_SIZE)?,
            row_payloads: AccountedBuffer::with_capacity(memory.clone(), VECTOR_SIZE)?,
            path_rows: AccountedBuffer::with_capacity(
                memory.clone(),
                if track_paths { VECTOR_SIZE } else { 0 },
            )?,
            path_payloads: AccountedBuffer::with_capacity(
                memory,
                if track_paths { VECTOR_SIZE } else { 0 },
            )?,
        })
    }

    pub fn clear(&mut self) {
        self.rows.clear();
        self.row_payloads.clear();
        self.path_rows.clear();
        self.path_payloads.clear();
    }

    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    pub fn is_full(&self) -> bool {
        self.rows.len() >= self.capacity_rows
    }

    pub fn push_row(
        &mut self,
        row: Vec<u64>,
        path: Option<MaterializedPath>,
    ) -> Result<EmitAction> {
        let row_payload = self.retain_payload(row.capacity() * size_of::<u64>())?;
        if self.track_paths {
            let path = path.expect("Graph path output requires a matching path row");
            let path_payload = self.retain_payload(materialized_path_payload_bytes(&path))?;
            if let Err(err) = self.path_rows.try_push(path) {
                drop(path_payload);
                drop(row_payload);
                return Err(err.into());
            }
            if let Err(err) = self.path_payloads.try_push(path_payload) {
                let _ = self.path_rows.pop();
                drop(row_payload);
                return Err(err.into());
            }
        } else {
            debug_assert!(path.is_none());
        }
        if let Err(err) = self.rows.try_push(row) {
            if self.track_paths {
                let _ = self.path_rows.pop();
                let _ = self.path_payloads.pop();
            }
            drop(row_payload);
            return Err(err.into());
        }
        if let Err(err) = self.row_payloads.try_push(row_payload) {
            let _ = self.rows.pop();
            if self.track_paths {
                let _ = self.path_rows.pop();
                let _ = self.path_payloads.pop();
            }
            return Err(err.into());
        }
        Ok(if self.is_full() {
            EmitAction::Yield
        } else {
            EmitAction::Continue
        })
    }

    pub fn take(&mut self) -> (Vec<Vec<u64>>, Vec<MaterializedPath>) {
        (
            self.rows.drain().collect(),
            self.path_rows.drain().collect(),
        )
    }

    fn retain_payload(&self, bytes: usize) -> Result<RetainedMemoryHandle> {
        Ok(RetainedMemoryHandle::new(self.memory.retain(bytes)?))
    }
}

fn materialized_path_payload_bytes(path: &MaterializedPath) -> usize {
    (path.vertices.capacity() + path.edges.capacity()) * size_of::<PathElementRef>()
}
