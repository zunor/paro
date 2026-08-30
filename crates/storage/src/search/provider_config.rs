// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Strict, versioned physical contracts shared by non-HNSW search providers.

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use paro_common::error::{self as paro_error, Result};

pub const FULLTEXT_PROVIDER_CONFIG_VERSION: u32 = 1;
pub const SPARSE_PROVIDER_CONFIG_VERSION: u32 = 1;

/// Shared strict boundary for durable provider configuration. New providers
/// must opt into version checking and semantic validation here instead of
/// growing another ad-hoc JSON fallback.
pub(crate) trait StrictProviderConfig: Serialize + DeserializeOwned + Sized {
    const PROVIDER_NAME: &'static str;
    const VERSION: u32;

    fn version(&self) -> u32;
    fn validate_semantics(&self) -> Result<()>;
}

pub(crate) fn decode_provider_config<T: StrictProviderConfig>(value: &Value) -> Result<T> {
    let config: T = serde_json::from_value(value.clone()).map_err(|err| {
        paro_error::invalid_input(format!(
            "invalid {} provider_config: {err}",
            T::PROVIDER_NAME
        ))
    })?;
    if config.version() != T::VERSION {
        return Err(paro_error::invalid_input(format!(
            "unsupported {} provider_config version {}, expected {}",
            T::PROVIDER_NAME,
            config.version(),
            T::VERSION
        )));
    }
    config.validate_semantics()?;
    Ok(config)
}

pub(crate) fn encode_provider_config<T: StrictProviderConfig>(config: &T) -> Result<Value> {
    config.validate_semantics()?;
    serde_json::to_value(config).map_err(|err| {
        paro_error::serialization_error(format!(
            "serialize {} provider_config: {err}",
            T::PROVIDER_NAME
        ))
    })
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FullTextProviderConfig {
    pub version: u32,
    pub config: String,
}

impl FullTextProviderConfig {
    pub fn from_value(value: &Value) -> Result<Self> {
        decode_provider_config(value)
    }

    pub fn to_value(&self) -> Result<Value> {
        encode_provider_config(self)
    }
}

impl StrictProviderConfig for FullTextProviderConfig {
    const PROVIDER_NAME: &'static str = "FullText";
    const VERSION: u32 = FULLTEXT_PROVIDER_CONFIG_VERSION;

    fn version(&self) -> u32 {
        self.version
    }

    fn validate_semantics(&self) -> Result<()> {
        crate::index::fulltext::tokenizer::TokenizerKind::from_config(&self.config).map(|_| ())
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
        decode_provider_config(value)
    }

    pub fn to_value(&self) -> Result<Value> {
        encode_provider_config(self)
    }
}

impl StrictProviderConfig for SparseProviderConfig {
    const PROVIDER_NAME: &'static str = "Sparse";
    const VERSION: u32 = SPARSE_PROVIDER_CONFIG_VERSION;

    fn version(&self) -> u32 {
        self.version
    }

    fn validate_semantics(&self) -> Result<()> {
        Ok(())
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
