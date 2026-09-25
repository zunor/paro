// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Filter combination, propagation, pushdown and pullup.

pub mod combiner;
pub mod propagate_result;
#[cfg(test)]
pub mod pullup;
pub mod pushdown;

pub(crate) mod column_transfer;
pub(crate) mod restriction;

pub(crate) mod canonical;
