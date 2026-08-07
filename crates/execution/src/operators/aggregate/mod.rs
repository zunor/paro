// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

pub mod hash;
pub mod perfect_hash;
pub mod ungrouped;

pub(crate) mod accounted_rows;
pub mod aggregate_kernel;
pub mod aggregate_object;
pub mod aggregate_state;
pub(crate) mod build_helpers;
pub(crate) mod distinct_helpers;
pub(crate) mod distinct_state;
pub(crate) mod group_hash;
pub(crate) mod group_key_codec;
pub mod grouped_aggregate_data;
pub mod grouped_aggregate_hashtable;
pub(crate) mod ordered_helpers;
pub(crate) mod output_filter;
pub(crate) mod payload_spill;
pub mod perfect_aggregate_hashtable;
pub(crate) mod perfect_hash_key;
pub mod radix_partitioned_aggregate_hashtable;
pub mod row_format;
pub mod state;
pub mod tuple_layout;
