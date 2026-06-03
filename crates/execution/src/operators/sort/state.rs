// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::collections::VecDeque;
use std::sync::Arc;

use paro_common::chunk::Chunk;
use paro_common::types::LogicalType;

use crate::expression_executor::executor::ExpressionExecutor;
use crate::runtime::breaker::SortSealedState;
use crate::sorting::sort_descriptor::Sort;
use crate::sorting::sorted_run::RunBuilder;
use crate::sorting::sorted_run_merger::SortedRunMergerLocalState;

use super::topn_heap::{TopNBoundaryValue, TopNHeap};

#[derive(Debug)]
pub struct SortEmitSourceLocal {
    pub state: Option<Arc<SortSealedState>>,
    pub current_position: usize,
    pub merger_lstate: SortedRunMergerLocalState,
}

impl Default for SortEmitSourceLocal {
    fn default() -> Self {
        Self {
            state: None,
            current_position: 0,
            merger_lstate: SortedRunMergerLocalState::new(),
        }
    }
}

#[derive(Debug, Default)]
pub struct TopNEmitSourceLocal {
    pub chunks: Option<Arc<[Chunk]>>,
    pub cursor: usize,
}

#[derive(Debug, Default)]
pub struct SortBuildSinkLocal {
    pub sort: Option<Arc<Sort>>,
    pub run_builder: Option<RunBuilder>,
    pub maximum_run_size: usize,
    pub external: bool,
    pub key_chunk: Option<Chunk>,
    pub payload_chunk: Option<Chunk>,
}

#[derive(Debug)]
pub struct TopNBuildSinkLocal {
    pub heap: TopNHeap,
    pub boundary: Arc<TopNBoundaryValue>,
    pub order_executor: ExpressionExecutor,
    pub order_types: Box<[LogicalType]>,
    pub sort_chunk: Chunk,
}

#[derive(Debug, Default)]
pub struct StreamingTopNTransformGlobal;

#[derive(Debug)]
pub struct StreamingTopNTransformLocal {
    pub heap: TopNHeap,
    pub order_executor: ExpressionExecutor,
    pub output_chunks: VecDeque<Chunk>,
    pub finalized: bool,
}
