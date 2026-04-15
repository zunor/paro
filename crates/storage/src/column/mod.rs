// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Storage-side column data collection substrate.

mod allocator;
mod collection;
mod consumer;
mod partitioned;
mod radix_partitioned;

pub use allocator::{ChunkManagementState, ColumnDataAllocator, ColumnDataAllocatorType};
pub use collection::{
    ColumnDataAppendState, ColumnDataCollection, ColumnDataLocalScanState,
    ColumnDataParallelScanState, ColumnDataScanState,
};
pub use consumer::{ColumnDataConsumer, ColumnDataConsumerScanState};
pub use partitioned::{
    ColumnPartitionIndexComputer, PartitionedColumnData, PartitionedColumnDataAppendState,
};
pub use radix_partitioned::{RadixPartitionedColumnData, RadixPartitioning};
