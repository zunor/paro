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
