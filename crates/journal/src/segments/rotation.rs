// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Rotation policy for physical WAL segments.

pub const DEFAULT_SEGMENT_ROTATION_BYTES: u64 = 64 * 1024 * 1024;

pub fn should_rotate_after_flush(current_size_bytes: u64, rotation_bytes: u64) -> bool {
    rotation_bytes != 0 && current_size_bytes >= rotation_bytes
}
