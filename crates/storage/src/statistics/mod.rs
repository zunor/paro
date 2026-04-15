// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! # Statistics
//!
//! Statistics for data pruning and optimization.
//!
//! ## Design
//! - Statistics track min/max/nulls for pruning
//! - BaseStatistics is the core class with type-specific stats
//! - SegmentStatistics is a simple wrapper around BaseStatistics
//! - NumericStats, StringStats for type-specific operations
//! - DistinctStatistics uses HyperLogLog for cardinality estimation
//! - ColumnStatistics combines BaseStatistics with DistinctStatistics
//! - TableStatistics manages statistics for all columns in a table

mod array_stats;
mod base_statistics;
mod column_statistics;
mod delete_statistics;
mod distinct_statistics;
mod fulltext_statistics;
mod index_statistics;
mod list_stats;
mod numeric_stats;
mod search_telemetry;
mod segment_statistics;
mod stats_trailer;
mod string_stats;
mod struct_stats;
mod table_statistics;
mod types;
mod vector_index_statistics;

pub use array_stats::{ArrayStatsData, ChildStats};
pub use base_statistics::{BaseStatistics, StatsData};
pub use column_statistics::ColumnStatistics;
pub use delete_statistics::DeleteStatistics;
pub use distinct_statistics::DistinctStatistics;
pub use fulltext_statistics::FullTextIndexStatistics;
pub use index_statistics::{IndexStatistics, IndexType, SegmentIndexStatistics};
pub use list_stats::{ListChildStats, ListStats, ListStatsData};
pub use numeric_stats::{NumericStats, NumericStatsData, NumericValueUnion};
pub use search_telemetry::{FullTextSearchTelemetry, HnswBatchTelemetry, SearchTelemetry};
pub use segment_statistics::SegmentStatistics;
pub(crate) use stats_trailer::{append_stats_trailer, split_stats_trailer};
pub use string_stats::{StringStats, StringStatsData, MAX_STRING_MINMAX_SIZE};
pub use struct_stats::StructStats;
pub use table_statistics::{TableStatistics, TableStatisticsLock};
pub use types::{StatisticsType, StatsInfo};
pub use vector_index_statistics::{HnswIndexStatistics, SparseIndexStatistics};
