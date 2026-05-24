// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use paro_common::chunk::Chunk;

#[derive(Debug, Default)]
pub struct WindowEmitSourceLocal {
    pub chunks: Option<Arc<[Chunk]>>,
    pub cursor: usize,
}

#[derive(Debug, Default)]
pub struct WindowBuildSinkLocal {
    pub chunks: Vec<Chunk>,
}

#[derive(Debug, Default)]
pub struct StreamingWindowTransformGlobal;

#[derive(Debug, Default)]
pub struct StreamingWindowTransformLocal {
    pub next_row_number: i64,
}
