// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Drop-released handles for retained payload bytes.

use paro_common::memory::MemoryReleaseHandle;

/// Owns a published retained allocation and releases it on drop.
#[derive(Debug)]
pub struct RetainedMemoryHandle {
    release: MemoryReleaseHandle,
}

impl RetainedMemoryHandle {
    pub fn new(release: MemoryReleaseHandle) -> Self {
        Self { release }
    }

    pub fn bytes(&self) -> usize {
        self.release.bytes()
    }
}

impl Drop for RetainedMemoryHandle {
    fn drop(&mut self) {
        self.release.release();
    }
}
