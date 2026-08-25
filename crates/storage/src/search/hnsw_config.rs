// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Versioned physical contract for an HNSW search definition.
//!
//! Catalog persistence remains JSON, but it is decoded into this type at the
//! definition boundary. Provider code must not read individual JSON fields or
//! invent fallback values.

use serde::{Deserialize, Serialize};
use serde_json::Value;

pub use crate::index::hnsw::types::{
    DEFAULT_HNSW_BUILD_SEED, DEFAULT_HNSW_EF_CONSTRUCT, DEFAULT_HNSW_EF_SEARCH,
    DEFAULT_HNSW_FILTERED_PLAIN_SCAN_THRESHOLD, DEFAULT_HNSW_FILTER_BLOCK_ROWS,
    DEFAULT_HNSW_FILTER_M, DEFAULT_HNSW_GRAPH_SCORED_POINTS_PER_EF,
    DEFAULT_HNSW_INDEXED_BASE_SCORES_PER_RANDOM_SCORE, DEFAULT_HNSW_M,
    DEFAULT_HNSW_PLAIN_SCAN_THRESHOLD, DEFAULT_HNSW_PROPOSAL_WAVE_SIZE,
    DEFAULT_HNSW_SEQUENTIAL_COVERING_SCORES_PER_RANDOM_SCORE, DEFAULT_HNSW_WARMUP_POINT_COUNT,
};
use crate::index::hnsw::{
    DistanceMetric, HnswBuildContract, HnswDistanceCostProfile, HnswFilterTopologyContract,
    HnswSearchPolicy, MAX_HNSW_FILTER_COLUMNS,
};
use paro_common::error::{self as paro_error, Result};

use super::provider_config::{
    decode_provider_config, encode_provider_config, StrictProviderConfig,
};

/// Version 12 adds a definition-pinned unique graph-score/ef cost calibrated
/// independently from maximum graph degree.
/// Version 11 makes exact/graph cost ratios an explicit, reproducible search
/// policy instead of deriving plans from process-global timing history.
/// Version 10 uses the configured construction beam on every HNSW layer.
/// Version 9 binds definitions to the chunk-authenticated HNSW artifact
/// generation. Artifact-envelope compatibility is versioned independently;
/// provider-config versions describe the definition and build contract rather
/// than the physical checksum hierarchy used by a particular binary.
pub const HNSW_PROVIDER_CONFIG_VERSION: u32 = 12;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HnswInlineConfig {
    pub enabled: bool,
    pub max_vector_count: u64,
    pub max_graph_memory_bytes: u64,
    pub max_dimension: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HnswProviderConfig {
    pub version: u32,
    pub dimension: u32,
    pub distance: DistanceMetric,
    /// Fixed-width durable fields. Runtime `usize` values are derived only
    /// after validation so catalog bytes have platform-independent meaning.
    pub m: u32,
    pub ef_construct: u32,
    pub ef_search: u32,
    pub plain_scan_threshold: u32,
    pub filtered_plain_scan_threshold: u32,
    pub sequential_covering_scores_per_random_score: u32,
    pub indexed_base_scores_per_random_score: u32,
    pub graph_scored_points_per_ef: u32,
    pub build_seed: u64,
    pub proposal_wave_size: u32,
    pub warmup_point_count: u32,
    pub filter_columns: Vec<u32>,
    pub filter_block_rows: u32,
    pub filter_m: u32,
    pub inline_threshold: HnswInlineConfig,
}

impl HnswProviderConfig {
    pub fn validated(self) -> Result<Self> {
        self.validate()?;
        Ok(self)
    }

    pub fn from_value(value: &Value) -> Result<Self> {
        decode_provider_config(value)
    }

    pub fn to_value(&self) -> Result<Value> {
        encode_provider_config(self)
    }

    pub fn validate(&self) -> Result<()> {
        if self.version != HNSW_PROVIDER_CONFIG_VERSION {
            return Err(paro_error::invalid_input(format!(
                "unsupported HNSW provider_config version {}, expected {}",
                self.version, HNSW_PROVIDER_CONFIG_VERSION
            )));
        }
        if self.dimension == 0 {
            return Err(paro_error::invalid_input(
                "HNSW provider_config dimension must be greater than zero",
            ));
        }
        if !(2..=1_024).contains(&self.m) {
            return Err(paro_error::invalid_input(format!(
                "HNSW m must be between 2 and 1024, got {}",
                self.m
            )));
        }
        if self.ef_construct < self.m || self.ef_construct > 1_000_000 {
            return Err(paro_error::invalid_input(format!(
                "HNSW ef_construct must be between m ({}) and 1000000, got {}",
                self.m, self.ef_construct
            )));
        }
        if !(1..=4_096).contains(&self.proposal_wave_size) {
            return Err(paro_error::invalid_input(format!(
                "HNSW proposal_wave_size must be between 1 and 4096, got {}",
                self.proposal_wave_size
            )));
        }
        if self.warmup_point_count > 1_000_000_000 {
            return Err(paro_error::invalid_input(format!(
                "HNSW warmup_point_count exceeds 1000000000, got {}",
                self.warmup_point_count
            )));
        }
        if self.ef_search == 0 || self.ef_search > 1_000_000 {
            return Err(paro_error::invalid_input(format!(
                "HNSW ef_search must be between 1 and 1000000, got {}",
                self.ef_search
            )));
        }
        for (name, ratio) in [
            (
                "sequential_covering_scores_per_random_score",
                self.sequential_covering_scores_per_random_score,
            ),
            (
                "indexed_base_scores_per_random_score",
                self.indexed_base_scores_per_random_score,
            ),
            (
                "graph_scored_points_per_ef",
                self.graph_scored_points_per_ef,
            ),
        ] {
            if !(1..=1_024).contains(&ratio) {
                return Err(paro_error::invalid_input(format!(
                    "HNSW {name} must be between 1 and 1024, got {ratio}"
                )));
            }
        }
        if self.filter_columns.len() > MAX_HNSW_FILTER_COLUMNS {
            return Err(paro_error::invalid_input(format!(
                "HNSW filter_columns supports at most {MAX_HNSW_FILTER_COLUMNS} columns"
            )));
        }
        if self
            .filter_columns
            .windows(2)
            .any(|pair| pair[0] >= pair[1])
        {
            return Err(paro_error::invalid_input(
                "HNSW filter_columns must be sorted and unique",
            ));
        }
        if self.filter_block_rows == 0 {
            return Err(paro_error::invalid_input(
                "HNSW filter_block_rows must be greater than zero",
            ));
        }
        if !(2..=64).contains(&self.filter_m) {
            return Err(paro_error::invalid_input(format!(
                "HNSW filter_m must be between 2 and 64, got {}",
                self.filter_m
            )));
        }
        if self.inline_threshold.enabled {
            if self.inline_threshold.max_vector_count == 0 {
                return Err(paro_error::invalid_input(
                    "enabled HNSW inline_threshold max_vector_count must be greater than zero",
                ));
            }
            if self.inline_threshold.max_graph_memory_bytes == 0 {
                return Err(paro_error::invalid_input(
                    "enabled HNSW inline_threshold max_graph_memory_bytes must be greater than zero",
                ));
            }
            if self.inline_threshold.max_dimension < self.dimension {
                return Err(paro_error::invalid_input(format!(
                    "enabled HNSW inline_threshold max_dimension {} is below vector dimension {}",
                    self.inline_threshold.max_dimension, self.dimension
                )));
            }
        } else if self.inline_threshold.max_vector_count != 0
            || self.inline_threshold.max_graph_memory_bytes != 0
            || self.inline_threshold.max_dimension != 0
        {
            return Err(paro_error::invalid_input(
                "disabled HNSW inline_threshold must use zero limits",
            ));
        }
        Ok(())
    }

    pub fn build_contract(&self) -> HnswBuildContract {
        // Provider validation has already constrained every durable-width
        // field; construct the physical contract without routing through the
        // legacy usize-based low-level configuration.
        HnswBuildContract {
            version: crate::index::hnsw::types::HNSW_BUILD_CONTRACT_VERSION,
            m: self.m,
            m0: self.m * 2,
            ef_construct: self.ef_construct,
            distance: self.distance,
            build_seed: self.build_seed,
            proposal_wave_size: self.proposal_wave_size,
            warmup_point_count: self.warmup_point_count,
            filter_topology: HnswFilterTopologyContract::from_columns(
                &self.filter_columns,
                self.filter_block_rows,
                self.filter_m,
            )
            .expect("validated HNSW filter topology must form a durable contract"),
        }
    }

    pub const fn search_policy(&self) -> HnswSearchPolicy {
        HnswSearchPolicy {
            ef_search: self.ef_search as usize,
            plain_scan_threshold: self.plain_scan_threshold as usize,
            filtered_plain_scan_threshold: self.filtered_plain_scan_threshold as usize,
            distance_cost: HnswDistanceCostProfile {
                sequential_covering_scores_per_random_score: self
                    .sequential_covering_scores_per_random_score,
                indexed_base_scores_per_random_score: self.indexed_base_scores_per_random_score,
                graph_scored_points_per_ef: self.graph_scored_points_per_ef,
            },
        }
    }
}

impl StrictProviderConfig for HnswProviderConfig {
    const PROVIDER_NAME: &'static str = "HNSW";
    const VERSION: u32 = HNSW_PROVIDER_CONFIG_VERSION;

    fn version(&self) -> u32 {
        self.version
    }

    fn validate_semantics(&self) -> Result<()> {
        self.validate()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn valid_config() -> HnswProviderConfig {
        HnswProviderConfig {
            version: HNSW_PROVIDER_CONFIG_VERSION,
            dimension: 100,
            distance: DistanceMetric::Euclidean,
            m: 24,
            ef_construct: 100,
            ef_search: 100,
            plain_scan_threshold: 10_000,
            filtered_plain_scan_threshold: 0,
            sequential_covering_scores_per_random_score:
                DEFAULT_HNSW_SEQUENTIAL_COVERING_SCORES_PER_RANDOM_SCORE,
            indexed_base_scores_per_random_score: DEFAULT_HNSW_INDEXED_BASE_SCORES_PER_RANDOM_SCORE,
            graph_scored_points_per_ef: DEFAULT_HNSW_GRAPH_SCORED_POINTS_PER_EF,
            build_seed: DEFAULT_HNSW_BUILD_SEED,
            proposal_wave_size: DEFAULT_HNSW_PROPOSAL_WAVE_SIZE,
            warmup_point_count: DEFAULT_HNSW_WARMUP_POINT_COUNT,
            filter_columns: Vec::new(),
            filter_block_rows: DEFAULT_HNSW_FILTER_BLOCK_ROWS,
            filter_m: DEFAULT_HNSW_FILTER_M,
            inline_threshold: HnswInlineConfig {
                enabled: true,
                max_vector_count: 4_096,
                max_graph_memory_bytes: 64 * 1024 * 1024,
                max_dimension: 1_536,
            },
        }
        .validated()
        .unwrap()
    }

    #[test]
    fn roundtrip_is_strict_and_typed() {
        let config = valid_config();
        let value = config.to_value().unwrap();
        assert_eq!(HnswProviderConfig::from_value(&value).unwrap(), config);

        let mut unknown = value;
        unknown["typo"] = json!(1);
        assert!(HnswProviderConfig::from_value(&unknown)
            .unwrap_err()
            .to_string()
            .contains("unknown field"));
    }

    #[test]
    fn missing_distance_is_rejected() {
        let mut value = valid_config().to_value().unwrap();
        value.as_object_mut().unwrap().remove("distance");
        assert!(HnswProviderConfig::from_value(&value)
            .unwrap_err()
            .to_string()
            .contains("missing field `distance`"));
    }

    #[test]
    fn search_policy_changes_do_not_change_the_build_contract() {
        let base = valid_config();
        let mut tuned = base.clone();
        tuned.ef_search = 240;
        tuned.plain_scan_threshold = 20_000;
        tuned.filtered_plain_scan_threshold = 128;
        tuned.graph_scored_points_per_ef = 20;
        tuned.validate().unwrap();

        assert_eq!(base.build_contract(), tuned.build_contract());
        assert_ne!(base.search_policy(), tuned.search_policy());
    }

    #[test]
    fn inline_admission_is_explicit_and_validated() {
        let mut invalid = valid_config();
        invalid.inline_threshold.max_dimension = invalid.dimension - 1;
        assert!(invalid
            .validate()
            .unwrap_err()
            .to_string()
            .contains("max_dimension"));

        invalid.inline_threshold = HnswInlineConfig {
            enabled: false,
            max_vector_count: 0,
            max_graph_memory_bytes: 0,
            max_dimension: 0,
        };
        invalid.validate().unwrap();
    }
}
