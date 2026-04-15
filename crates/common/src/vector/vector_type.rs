// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! VectorType enum - how data is physically stored.

/// How the vector data is physically stored.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VectorType {
    /// Standard uncompressed vector - array of values
    Flat,
    /// Single constant value repeated for all rows
    Constant,
    /// Selection vector on top of another vector (dictionary encoding)
    Dictionary,
    /// Sequence with start and increment
    Sequence,
}
