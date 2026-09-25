// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Join-related optimization passes.

#[cfg(test)]
pub mod elimination;
pub mod mixed_predicates;
pub mod null_rejected_equality;
