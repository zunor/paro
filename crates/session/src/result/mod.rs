// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Query result types and sinks used by session execution.

pub(crate) mod collecting_sink;
pub mod profiler;
pub mod progress;
pub(crate) mod query;
pub(crate) mod retained_store;
pub(crate) mod sink;
