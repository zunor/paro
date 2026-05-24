// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize};
use std::sync::Mutex;

use paro_common::chunk::Chunk;
use paro_function::copy::{CopyToGlobalState, CopyToLocalState};

#[derive(Debug)]
pub struct DmlSinkGlobal {
    pub affected_count: AtomicU64,
    pub append_lock: Mutex<()>,
    pub full_table_delete_executed: AtomicBool,
}

impl Default for DmlSinkGlobal {
    fn default() -> Self {
        Self {
            affected_count: AtomicU64::new(0),
            append_lock: Mutex::new(()),
            full_table_delete_executed: AtomicBool::new(false),
        }
    }
}

#[derive(Debug)]
pub struct InsertSinkLocal {
    pub initialized: bool,
    pub copy_buffering_enabled: bool,
    pub copy_buffer_size: usize,
    pub copy_flush_threads: usize,
    pub buffered_chunks: Vec<Chunk>,
    pub buffered_rows: usize,
}

impl Default for InsertSinkLocal {
    fn default() -> Self {
        Self {
            initialized: false,
            copy_buffering_enabled: false,
            copy_buffer_size: 8192,
            copy_flush_threads: 1,
            buffered_chunks: Vec::new(),
            buffered_rows: 0,
        }
    }
}

#[derive(Debug, Default)]
pub struct EmptyDmlSinkLocal;

pub struct CopyToSinkGlobal {
    pub row_count: AtomicU64,
    pub per_thread_output: bool,
    pub global_state: Option<Mutex<Box<dyn CopyToGlobalState>>>,
    pub next_file_id: AtomicUsize,
}

impl fmt::Debug for CopyToSinkGlobal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CopyToSinkGlobal")
            .field("row_count", &self.row_count)
            .field("per_thread_output", &self.per_thread_output)
            .field("has_global_state", &self.global_state.is_some())
            .finish()
    }
}

pub struct CopyToSinkLocal {
    pub local_state: Box<dyn CopyToLocalState>,
    pub thread_global_state: Option<Box<dyn CopyToGlobalState>>,
}

impl fmt::Debug for CopyToSinkLocal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CopyToSinkLocal")
            .field(
                "has_thread_global_state",
                &self.thread_global_state.is_some(),
            )
            .finish()
    }
}
