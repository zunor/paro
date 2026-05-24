// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

pub(crate) mod build;
pub(crate) mod emit;

#[cfg(test)]
mod tests;

pub use build::HashAggregateBuildSinkExec;
pub use emit::HashAggregateEmitSourceExec;
