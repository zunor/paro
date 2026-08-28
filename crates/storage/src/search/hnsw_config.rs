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
    DEFAULT_HNSW_EXACT_F32_DIMENSION_COST_UNITS, DEFAULT_HNSW_FILTER_BLOCK_ROWS,
    DEFAULT_HNSW_FILTER_M, DEFAULT_HNSW_GRAPH_SCORED_POINTS_PER_EF, DEFAULT_HNSW_M,
    DEFAULT_HNSW_PROPOSAL_WAVE_SIZE, DEFAULT_HNSW_RANDOM_ACCESS_COST_UNITS,
    DEFAULT_HNSW_SEQUENTIAL_DIMENSION_COST_UNITS, DEFAULT_HNSW_SYMMETRIC_I16_DIMENSION_COST_UNITS,
    DEFAULT_HNSW_WARMUP_POINT_COUNT, HNSW_BUILT_IN_DISTANCE_COST_REVISION,
};
use crate::index::hnsw::{
    DistanceMetric, HnswBuildContract, HnswBuildVectorEncoding, HnswDistanceCostProfile,
    HnswDistanceCostProfileSource, HnswFilterTopologyContract, HnswRerankPolicy, HnswSearchPolicy,
    MAX_HNSW_FILTER_COLUMNS,
};
use paro_common::error::{self as paro_error, Result};

use super::provider_config::{
    decode_provider_config, encode_provider_config, StrictProviderConfig,
};

/// Version 21 separates dimension-aware maintenance batching and ingest
/// backpressure from the query executor's exact-tail merge budget. A query
/// policy is not a valid build quantum: coupling them produced thousands of
/// tiny graphs during sustained ingest.
/// Version 21 makes incremental graph build quanta, backpressure, and
/// levelled-compaction fan-out a dimension-aware durable policy. Version 20
/// makes the exact rerank window an explicit search policy rather
/// than inferring it from the graph encoding and ef. Version 19 makes compact
/// encoding and its non-zero routing dimension an
/// atomic sum type and replaces reference-dimension cost ratios with direct,
/// encoding-aware physical work units. Version 18 upgrades compact routing to
/// symmetric i16 so ordered geometry cannot collapse into an i8 codebook.
/// Version 17 makes the construction
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
pub const HNSW_PROVIDER_CONFIG_VERSION: u32 = 21;

pub const DEFAULT_HNSW_MAINTENANCE_TARGET_VECTOR_BYTES: u64 = 8 * 1024 * 1024;
pub const DEFAULT_HNSW_MAINTENANCE_MAX_PENDING_VECTOR_BYTES: u64 = 256 * 1024 * 1024;
pub const DEFAULT_HNSW_MAINTENANCE_COMPACTION_FANOUT: u32 = 8;

/// Definition-pinned batching policy for derived HNSW maintenance.
///
/// Values are expressed in canonical vector bytes rather than rows so one
/// policy has comparable build-memory and publication meaning for 32d and
/// 768d vectors. Runtime derives row watermarks from the durable dimension.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HnswMaintenancePolicy {
    pub target_vector_bytes: u64,
    pub max_pending_vector_bytes: u64,
    pub compaction_fanout: u32,
}

impl Default for HnswMaintenancePolicy {
    fn default() -> Self {
        Self {
            target_vector_bytes: DEFAULT_HNSW_MAINTENANCE_TARGET_VECTOR_BYTES,
            max_pending_vector_bytes: DEFAULT_HNSW_MAINTENANCE_MAX_PENDING_VECTOR_BYTES,
            compaction_fanout: DEFAULT_HNSW_MAINTENANCE_COMPACTION_FANOUT,
        }
    }
}

impl HnswMaintenancePolicy {
    fn bytes_per_vector(dimension: u32) -> u64 {
        u64::from(dimension.max(1)).saturating_mul(std::mem::size_of::<f32>() as u64)
    }

    pub fn target_rows(self, dimension: u32) -> u64 {
        self.target_vector_bytes
            .div_ceil(Self::bytes_per_vector(dimension))
            .max(1)
    }

    pub fn max_pending_rows(self, dimension: u32) -> u64 {
        self.max_pending_vector_bytes
            .div_ceil(Self::bytes_per_vector(dimension))
            .max(self.target_rows(dimension))
    }

    pub fn vector_bytes(self, dimension: u32, rows: u64) -> u64 {
        rows.saturating_mul(Self::bytes_per_vector(dimension))
    }
}

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
    /// Fixed-width durable fields. Runtime `usize` values are derived only
    /// after validation so catalog bytes have platform-independent meaning.
    pub m: u32,
    pub ef_construct: u32,
    pub ef_search: u32,
    pub rerank_policy: HnswRerankPolicy,
    pub distance_cost: HnswDistanceCostProfile,
    pub maintenance: HnswMaintenancePolicy,
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
        if let Some(routing_dimensions) = self.build_vector_encoding.routing_dimensions() {
            if u32::from(routing_dimensions) > self.dimension {
                return Err(paro_error::invalid_input(format!(
                    "symmetric_i16 HNSW routing dimensions must not exceed the source dimension: got {} for dimension {}",
                    routing_dimensions,
                    self.dimension
                )));
            }
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
        if let HnswRerankPolicy::Fixed { candidates } = self.rerank_policy {
            if candidates.get() > 1_000_000 {
                return Err(paro_error::invalid_input(format!(
                    "HNSW fixed rerank window must not exceed 1000000, got {}",
                    candidates.get()
                )));
            }
        }
        for (name, units) in [
            (
                "random_access_cost_units",
                self.distance_cost.random_access_cost_units,
            ),
            (
                "exact_f32_dimension_cost_units",
                self.distance_cost.exact_f32_dimension_cost_units,
            ),
            (
                "sequential_dimension_cost_units",
                self.distance_cost.sequential_dimension_cost_units,
            ),
            (
                "symmetric_i16_dimension_cost_units",
                self.distance_cost.symmetric_i16_dimension_cost_units,
            ),
            (
                "graph_scored_points_per_ef",
                self.distance_cost.graph_scored_points_per_ef,
            ),
        ] {
            if !(1..=1_000_000).contains(&units) {
                return Err(paro_error::invalid_input(format!(
                    "HNSW {name} must be between 1 and 1000000, got {units}"
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
        if self.maintenance.target_vector_bytes == 0 {
            return Err(paro_error::invalid_input(
                "HNSW maintenance target_vector_bytes must be greater than zero",
            ));
        }
        if self.maintenance.max_pending_vector_bytes < self.maintenance.target_vector_bytes {
            return Err(paro_error::invalid_input(
                "HNSW maintenance max_pending_vector_bytes must be at least target_vector_bytes",
            ));
        }
        if self.maintenance.compaction_fanout < 2 {
            return Err(paro_error::invalid_input(
                "HNSW maintenance compaction_fanout must be at least 2",
            ));
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
            rerank_policy: self.rerank_policy,
            distance_cost: self.distance_cost,
            vector_encoding: self.build_vector_encoding,
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
            build_vector_encoding: HnswBuildVectorEncoding::symmetric_i16(100).unwrap(),
            m: 24,
            ef_construct: 100,
            ef_search: 100,
            rerank_policy: HnswRerankPolicy::Ef,
            distance_cost: HnswDistanceCostProfile::default(),
            maintenance: HnswMaintenancePolicy::default(),
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
    fn compact_encoding_cannot_deserialize_without_a_nonzero_dimension() {
        let mut value = valid_config().to_value().unwrap();
        value["build_vector_encoding"] = json!({ "symmetric_i16": { "routing_dimensions": 0 } });
        assert!(HnswProviderConfig::from_value(&value).is_err());
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

    #[test]
    fn maintenance_rows_are_dimension_aware_and_reproducible() {
        let policy = HnswMaintenancePolicy::default();
        assert_eq!(policy.target_rows(128), 16_384);
        assert_eq!(policy.max_pending_rows(128), 524_288);
        assert_eq!(policy.target_rows(768), 2_731);
        assert_eq!(policy.max_pending_rows(768), 87_382);
        assert_eq!(policy.vector_bytes(768, policy.target_rows(768)), 8_389_632);
    }

    #[test]
    fn maintenance_high_watermark_cannot_precede_the_build_quantum() {
        let mut invalid = valid_config();
        invalid.maintenance = HnswMaintenancePolicy {
            target_vector_bytes: 1024,
            max_pending_vector_bytes: 512,
            compaction_fanout: 8,
        };
        assert!(invalid
            .validate()
            .unwrap_err()
            .to_string()
            .contains("must be at least target_vector_bytes"));
    }
}
