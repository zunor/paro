// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! PostgreSQL-compatible protocol adapters.

pub(crate) mod copy;
pub(crate) mod data_row;
pub(crate) mod extended;
pub(crate) mod result;
pub(crate) mod simple;
pub(crate) mod value_format;
