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
    DEFAULT_HNSW_BUILD_SEED, DEFAULT_HNSW_DISTANCE_COST_REFERENCE_DIMENSION,
    DEFAULT_HNSW_EF_CONSTRUCT, DEFAULT_HNSW_EF_SEARCH, DEFAULT_HNSW_FILTER_BLOCK_ROWS,
    DEFAULT_HNSW_FILTER_M, DEFAULT_HNSW_GRAPH_SCORED_POINTS_PER_EF,
    DEFAULT_HNSW_INDEXED_BASE_SCORES_PER_RANDOM_SCORE, DEFAULT_HNSW_M,
    DEFAULT_HNSW_PROPOSAL_WAVE_SIZE, DEFAULT_HNSW_SEQUENTIAL_COVERING_SCORES_PER_RANDOM_SCORE,
    DEFAULT_HNSW_WARMUP_POINT_COUNT, HNSW_BUILT_IN_DISTANCE_COST_REVISION,
};
use crate::index::hnsw::{
    DistanceMetric, HnswBuildContract, HnswBuildVectorEncoding, HnswDistanceCostProfile,
    HnswDistanceCostProfileSource, HnswFilterTopologyContract, HnswSearchPolicy,
    MAX_HNSW_FILTER_COLUMNS,
};
use paro_common::error::{self as paro_error, Result};

use super::provider_config::{
    decode_provider_config, encode_provider_config, StrictProviderConfig,
};

/// Version 18 upgrades compact routing to symmetric i16 so ordered geometry
/// cannot collapse into an i8 codebook. Version 17 makes the construction
/// routing-vector encoding an explicit,
/// durable definition choice. Version 16 pins the dimension-aware distance
/// cost model and its provenance to the definition.
/// Version 15 binds construction to canonical unordered point-pair scoring so
/// cosine topology cannot vary with heuristic operand order.
/// Version 14 removes cardinality thresholds from search policy. Exact versus
/// graph selection is now derived exclusively from the definition-pinned
/// physical cost profile, effective ef, executable graph-pass count, and
/// exact-scan layout.
/// Version 13 makes the distance-cost profile an atomic, provenance-bearing
/// contract. Custom coefficients require a non-zero offline calibration id;
/// partial overrides and unlabeled tuning are rejected.
/// Version 12 adds a definition-pinned unique graph-score/ef cost calibrated
/// independently from maximum graph degree.
/// Version 11 makes exact/graph cost ratios an explicit, reproducible search
/// policy instead of deriving plans from process-global timing history.
/// Version 10 uses the configured construction beam on every HNSW layer.
/// Version 9 binds definitions to the chunk-authenticated HNSW artifact
/// generation. Artifact-envelope compatibility is versioned independently;
/// provider-config versions describe the definition and build contract rather
/// than the physical checksum hierarchy used by a particular binary.
pub const HNSW_PROVIDER_CONFIG_VERSION: u32 = 18;

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
    pub build_vector_encoding: HnswBuildVectorEncoding,
    pub build_routing_dimensions: u32,
    /// Fixed-width durable fields. Runtime `usize` values are derived only
    /// after validation so catalog bytes have platform-independent meaning.
    pub m: u32,
    pub ef_construct: u32,
    pub ef_search: u32,
    pub distance_cost: HnswDistanceCostProfile,
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
        match self.build_vector_encoding {
            HnswBuildVectorEncoding::ExactF32 if self.build_routing_dimensions != 0 => {
                return Err(paro_error::invalid_input(
                    "exact_f32 HNSW construction does not accept build_routing_dimensions",
                ));
            }
            HnswBuildVectorEncoding::SymmetricI16
                if self.build_routing_dimensions == 0
                    || self.build_routing_dimensions > self.dimension
                    || self.build_routing_dimensions > u16::MAX as u32 =>
            {
                return Err(paro_error::invalid_input(format!(
                    "symmetric_i16 HNSW build_routing_dimensions must be between 1 and min(dimension, {}), got {} for dimension {}",
                    u16::MAX,
                    self.build_routing_dimensions,
                    self.dimension
                )));
            }
            _ => {}
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
        if self.distance_cost.reference_dimension == 0
            || self.distance_cost.reference_dimension > 1_000_000
        {
            return Err(paro_error::invalid_input(format!(
                "HNSW distance_cost_reference_dimension must be between 1 and 1000000, got {}",
                self.distance_cost.reference_dimension
            )));
        }
        for (name, ratio) in [
            (
                "sequential_covering_scores_per_random_score",
                self.distance_cost
                    .sequential_covering_scores_per_random_score,
            ),
            (
                "indexed_base_scores_per_random_score",
                self.distance_cost.indexed_base_scores_per_random_score,
            ),
            (
                "graph_scored_points_per_ef",
                self.distance_cost.graph_scored_points_per_ef,
            ),
        ] {
            if !(1..=1_024).contains(&ratio) {
                return Err(paro_error::invalid_input(format!(
                    "HNSW {name} must be between 1 and 1024, got {ratio}"
                )));
            }
        }
        match self.distance_cost.source {
            HnswDistanceCostProfileSource::BuiltIn { revision } => {
                if revision != HNSW_BUILT_IN_DISTANCE_COST_REVISION
                    || self.distance_cost != HnswDistanceCostProfile::default()
                {
                    return Err(paro_error::invalid_input(format!(
                        "HNSW built-in distance-cost profile must exactly match revision {HNSW_BUILT_IN_DISTANCE_COST_REVISION}"
                    )));
                }
            }
            HnswDistanceCostProfileSource::OfflineCalibration { calibration_id } => {
                if calibration_id == 0 {
                    return Err(paro_error::invalid_input(
                        "HNSW offline distance-cost calibration id must be non-zero",
                    ));
                }
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
            vector_encoding: self.build_vector_encoding,
            routing_dimensions: self.build_routing_dimensions,
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
            distance_cost: self.distance_cost,
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
            build_vector_encoding: HnswBuildVectorEncoding::SymmetricI16,
            build_routing_dimensions: 100,
            m: 24,
            ef_construct: 100,
            ef_search: 100,
            distance_cost: HnswDistanceCostProfile::default(),
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
        tuned.distance_cost = HnswDistanceCostProfile {
            source: HnswDistanceCostProfileSource::OfflineCalibration { calibration_id: 1 },
            graph_scored_points_per_ef: 20,
            ..HnswDistanceCostProfile::default()
        };
        tuned.validate().unwrap();

        assert_eq!(base.build_contract(), tuned.build_contract());
        assert_ne!(base.search_policy(), tuned.search_policy());
    }

    #[test]
    fn distance_cost_provenance_cannot_mislabel_custom_coefficients() {
        let mut mislabeled = valid_config();
        mislabeled.distance_cost.graph_scored_points_per_ef = 20;
        assert!(mislabeled
            .validate()
            .unwrap_err()
            .to_string()
            .contains("must exactly match revision"));

        let mut unidentified = valid_config();
        unidentified.distance_cost = HnswDistanceCostProfile {
            source: HnswDistanceCostProfileSource::OfflineCalibration { calibration_id: 0 },
            ..HnswDistanceCostProfile::default()
        };
        assert!(unidentified
            .validate()
            .unwrap_err()
            .to_string()
            .contains("must be non-zero"));
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
