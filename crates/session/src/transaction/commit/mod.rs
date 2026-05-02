// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! SQL commit pipeline facade.

mod ddl_publish;
mod errors;
mod job_builder;
mod pipeline;
mod prepare;
mod publish_plan;

pub use errors::CommitFailure;
pub use pipeline::{CommitOutcome, CommitPipeline};
