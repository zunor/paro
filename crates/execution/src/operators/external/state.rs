// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::collections::VecDeque;
use std::sync::Arc;

use paro_common::chunk::Chunk;

use crate::runtime::breaker::ExternalTableHandle;

use super::batching::SubmissionBatchPolicy;

#[derive(Debug)]
pub struct ExternalTableSourceGlobal {
    pub handle: Arc<ExternalTableHandle>,
}

#[derive(Debug, Default)]
pub struct ExternalTableSourceLocal;

#[derive(Debug)]
pub struct ExternalTableSinkGlobal {
    pub handle: Arc<ExternalTableHandle>,
}

#[derive(Debug, Default)]
pub struct ExternalTableSinkLocal {
    pub next_batch_id: u64,
    pub next_partition_id: u64,
}

#[derive(Debug)]
pub struct ExternalProjectTransformGlobal {
    pub batch_policy: SubmissionBatchPolicy,
}

#[derive(Debug, Default)]
pub struct ExternalProjectTransformLocal {
    pub ready: VecDeque<Chunk>,
    pub next_batch_id: u64,
}
