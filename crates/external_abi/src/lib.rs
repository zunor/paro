// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Shared host/worker ABI descriptors for external routine execution.

pub mod descriptor;
pub mod encoding;
pub mod layout;
pub mod lease;
pub mod types;

pub use descriptor::{ColumnDescriptor, ColumnDescriptorError};
pub use encoding::{ColumnEncoding, ColumnPopulationMode};
pub use layout::{BufferDevice, BufferLease, ColumnLayout, OffsetWidth, ScalarValueRef};
pub use lease::{ColumnBatchLease, LeaseError, LeaseOwnership, LeaseState, CURRENT_ABI_VERSION};
pub use types::{AbiLogicalType, AbiStructField};
