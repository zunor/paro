// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use async_trait::async_trait;
use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::types::LogicalType;
use paro_function::copy::CopyOptions;
use tokio_util::bytes::Bytes;

use crate::result::sink::ResultSink;

/// Protocol-facing COPY OUT adapter used by front-end execution.
#[async_trait]
pub trait CopyProtocolSink: Send {
    async fn start_copy_out(&mut self, names: &[String], types: &[LogicalType]) -> Result<()>;
    async fn push_copy_rows(&mut self, chunk: &Chunk) -> Result<()>;
    async fn finish_copy_out(&mut self) -> Result<()>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CopyInSpec {
    pub overall_format: i8,
    pub column_formats: Vec<i16>,
}

/// Protocol-facing COPY IN adapter used by front-end execution.
#[async_trait]
pub trait CopyProtocolSource: Send {
    async fn begin_copy_in(&mut self, spec: &CopyInSpec) -> Result<()>;
    async fn next_chunk(&mut self) -> Result<Option<Bytes>>;
}

/// Result sink extension that optionally exposes COPY protocol adapters.
pub trait ProtocolResultSink: ResultSink {
    fn create_copy_out_sink(
        &mut self,
        _options: &CopyOptions,
    ) -> Result<Box<dyn CopyProtocolSink + '_>> {
        Err(paro_error::not_supported(
            "COPY TO STDOUT is not available in this context",
        ))
    }

    fn create_copy_in_source(&mut self) -> Result<Box<dyn CopyProtocolSource + '_>> {
        Err(paro_error::not_supported(
            "COPY FROM STDIN is not available in this context",
        ))
    }
}
