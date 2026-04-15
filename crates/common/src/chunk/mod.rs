// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

#[allow(clippy::module_inception)]
mod chunk;
mod ops;

#[cfg(test)]
mod tests;

pub use chunk::Chunk;
