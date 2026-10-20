// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Typed Arrow boundary and owned Python Flight service.

pub mod client;
pub mod codec;
pub mod definition;
mod process;

pub const MAX_BATCH_BYTES: usize = 16 << 20;
pub const MAX_BATCH_ROWS: usize = 65_536;
pub const MAX_CONTROL_BYTES: usize = 1 << 20;

/// Arrow builders and wire buffers charged outside the native allocator.
pub const TRANSPORT_WORKING_SET_BYTES: usize = (MAX_BATCH_BYTES + MAX_CONTROL_BYTES) * 4;
