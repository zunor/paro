// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Strict, versioned physical contracts shared by non-HNSW search providers.

use serde::{Deserialize, Serialize};
use serde_json::Value;

use paro_common::error::{self as paro_error, Result};

pub const FULLTEXT_PROVIDER_CONFIG_VERSION: u32 = 1;
pub const SPARSE_PROVIDER_CONFIG_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FullTextProviderConfig {
    pub version: u32,
    pub config: String,
}

impl FullTextProviderConfig {
    pub fn from_value(value: &Value) -> Result<Self> {
        let config: Self = serde_json::from_value(value.clone()).map_err(|err| {
            paro_error::invalid_input(format!("invalid FullText provider_config: {err}"))
        })?;
        if config.version != FULLTEXT_PROVIDER_CONFIG_VERSION {
            return Err(paro_error::invalid_input(format!(
                "unsupported FullText provider_config version {}, expected {}",
                config.version, FULLTEXT_PROVIDER_CONFIG_VERSION
            )));
        }
        crate::index::fulltext::tokenizer::TokenizerKind::from_config(&config.config)?;
        Ok(config)
    }

    pub fn to_value(&self) -> Result<Value> {
        serde_json::to_value(self).map_err(|err| {
            paro_error::serialization_error(format!("serialize FullText provider_config: {err}"))
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SparseProviderConfig {
    pub version: u32,
    pub physical_encoding: SparsePhysicalEncoding,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SparsePhysicalEncoding {
    #[serde(rename = "binary-v1")]
    BinaryV1,
}

impl SparseProviderConfig {
    pub fn from_value(value: &Value) -> Result<Self> {
        let config: Self = serde_json::from_value(value.clone()).map_err(|err| {
            paro_error::invalid_input(format!("invalid Sparse provider_config: {err}"))
        })?;
        if config.version != SPARSE_PROVIDER_CONFIG_VERSION {
            return Err(paro_error::invalid_input(format!(
                "unsupported Sparse provider_config version {}, expected {}",
                config.version, SPARSE_PROVIDER_CONFIG_VERSION
            )));
        }
        Ok(config)
    }

    pub fn to_value(&self) -> Result<Value> {
        serde_json::to_value(self).map_err(|err| {
            paro_error::serialization_error(format!("serialize Sparse provider_config: {err}"))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn fulltext_config_is_versioned_and_rejects_unknown_fields() {
        let config = FullTextProviderConfig::from_value(&json!({
            "version": 1,
            "config": "simple"
        }))
        .unwrap();
        assert_eq!(config.config, "simple");
        assert!(FullTextProviderConfig::from_value(&json!({
            "version": 1,
            "config": "simple",
            "typo": true
        }))
        .is_err());
    }

    #[test]
    fn sparse_config_has_one_canonical_physical_encoding() {
        let config = SparseProviderConfig::from_value(&json!({
            "version": 1,
            "physical_encoding": "binary-v1"
        }))
        .unwrap();
        assert_eq!(config.physical_encoding, SparsePhysicalEncoding::BinaryV1);
        assert!(SparseProviderConfig::from_value(&json!({
            "version": 1,
            "physical_encoding": "typed-binary-v1"
        }))
        .is_err());
    }
}
