// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

/// Cache-line aligned wrapper for hot registry slots and shard summaries.
#[repr(align(64))]
#[derive(Debug)]
pub(crate) struct CachePadded<T>(pub(crate) T);

impl<T> CachePadded<T> {
    #[inline]
    pub(crate) const fn new(value: T) -> Self {
        Self(value)
    }
}
