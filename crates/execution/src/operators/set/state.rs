// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use paro_common::chunk::Chunk;

use crate::expression_executor::executor::ExpressionExecutor;
use crate::runtime::breaker::{DelimHandle, RecursiveDedupSet, RecursiveTableHandle};

#[derive(Debug, Default)]
pub struct CteScanSourceLocal {
    pub chunks: Option<Arc<[Chunk]>>,
    pub cursor: usize,
}

#[derive(Debug, Default)]
pub struct DelimScanSourceLocal {
    pub chunks: Option<Arc<[Chunk]>>,
    pub cursor: usize,
}

#[derive(Debug, Default)]
pub struct RecursiveTableScanSourceLocal {
    pub chunks: Option<Vec<Chunk>>,
    pub cursor: usize,
}

#[derive(Debug, Default)]
pub struct SetOperationEmitSourceLocal {
    pub chunks: Option<Arc<[Chunk]>>,
    pub cursor: usize,
}

#[derive(Debug, Default)]
pub struct CteMaterializeSinkLocal {
    pub chunks: Vec<Chunk>,
}

#[derive(Debug, Default)]
pub struct SetOperationInputSinkLocal {
    pub chunks: Vec<Chunk>,
}

#[derive(Debug)]
pub struct DelimCaptureSinkGlobal {
    pub values: Arc<DelimHandle>,
    pub cached_outer: Option<Arc<DelimHandle>>,
}

#[derive(Debug)]
pub struct DelimCaptureSinkLocal {
    pub key_executor: ExpressionExecutor,
    pub key_chunk: Chunk,
    pub value_chunks: Vec<Chunk>,
    pub cached_outer_chunks: Vec<Chunk>,
}

#[derive(Debug)]
pub struct RecursiveTableAppendSinkGlobal {
    pub target: Arc<RecursiveTableHandle>,
    pub dedup: Option<Arc<RecursiveDedupSet>>,
}

#[derive(Debug, Default)]
pub struct RecursiveTableAppendSinkLocal {
    pub chunks: Vec<Chunk>,
}
