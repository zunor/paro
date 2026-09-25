// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::any::Any;
use std::fmt::Debug;
use std::fmt::Formatter;

use paro_common::chunk::Chunk;
use paro_common::error::Result;
use paro_common::types::LogicalType;

use crate::table::{TableFunction, TableFunctionBindData};

pub mod csv;
pub mod json;
pub mod options;

pub use options::{CopyFormat, CopyOptions, ForceQuoteOption};

/// Physical input selected by a COPY FROM statement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CopyFromSource {
    File(String),
    Stdin,
}

impl CopyFromSource {
    pub(crate) fn identity_value(&self) -> serde_json::Value {
        match self {
            Self::File(path) => serde_json::json!(["file", path]),
            Self::Stdin => serde_json::json!(["stdin"]),
        }
    }
}

/// Internal options for CopyFunction, produced during the bind phase.
pub trait CopyFunctionBindData: Send + Sync + Debug {
    fn as_any(&self) -> &dyn Any;
    /// Versioned semantic binding owned by the format, excluding open files
    /// and execution state. Formats must encode every output-affecting option.
    fn canonical_plan_payload(&self) -> Vec<u8>;
}

pub trait CopyToGlobalState: Send + Sync {
    fn as_any(&self) -> &dyn Any;
    fn as_any_mut(&mut self) -> &mut dyn Any;
}

pub trait CopyToLocalState: Send + Sync {
    fn as_any(&self) -> &dyn Any;
    fn as_any_mut(&mut self) -> &mut dyn Any;
}

#[derive(Clone, Debug)]
pub struct CopyToFunction {
    pub copy_to_bind:
        fn(&CopyOptions, &[String], &[LogicalType]) -> Result<Box<dyn CopyFunctionBindData>>,
    pub copy_to_initialize_global:
        fn(&dyn CopyFunctionBindData, &str) -> Result<Box<dyn CopyToGlobalState>>,
    pub copy_to_initialize_local:
        fn(&dyn CopyFunctionBindData) -> Result<Box<dyn CopyToLocalState>>,
    pub copy_to_sink: fn(
        &dyn CopyFunctionBindData,
        &mut dyn CopyToGlobalState,
        &mut dyn CopyToLocalState,
        &Chunk,
    ) -> Result<()>,
    pub copy_to_combine: fn(
        &dyn CopyFunctionBindData,
        &mut dyn CopyToGlobalState,
        &mut dyn CopyToLocalState,
    ) -> Result<()>,
    pub copy_to_finalize: fn(&dyn CopyFunctionBindData, &mut dyn CopyToGlobalState) -> Result<()>,
}

#[derive(Clone, Debug)]
pub struct CopyFromFunction {
    pub copy_from_bind: fn(
        CopyFromSource,
        &CopyOptions,
        &[String],
        &[LogicalType],
    ) -> Result<Box<dyn TableFunctionBindData>>,
    pub copy_from_function: TableFunction,
}

/// Format-driven COPY capabilities. A format advertises each direction
/// explicitly; unsupported operations cannot carry unrelated callback tables.
#[derive(Clone)]
pub struct CopyFunction {
    pub name: String,
    pub copy_to: Option<CopyToFunction>,
    pub copy_from: Option<CopyFromFunction>,

    pub extension: String,
}

impl std::fmt::Debug for CopyFunction {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CopyFunction")
            .field("name", &self.name)
            .field("supports_copy_to", &self.copy_to.is_some())
            .field("supports_copy_from", &self.copy_from.is_some())
            .field("extension", &self.extension)
            .finish()
    }
}

pub fn register_copy_functions() -> Vec<CopyFunction> {
    let mut functions = csv::register_copy_functions();
    functions.extend(json::register_copy_functions());
    functions
}

#[cfg(test)]
mod identity_tests {
    use super::*;

    #[test]
    fn bound_copy_payload_is_stable_and_distinguishes_formats_and_types() {
        let names = vec!["a".to_string()];
        let types = vec![LogicalType::Integer];
        let mut input_payloads = std::collections::BTreeSet::new();
        let mut output_payloads = std::collections::BTreeSet::new();
        for function in register_copy_functions() {
            let mut options = CopyOptions::default();
            options.format = CopyFormat::parse(&function.name).unwrap();
            if let Some(input) = function.copy_from {
                let bind = |types: &[LogicalType]| {
                    (input.copy_from_bind)(CopyFromSource::Stdin, &options, &names, types).unwrap()
                };
                let payload = bind(&types).canonical_plan_payload().unwrap();
                assert_eq!(payload, bind(&types).canonical_plan_payload().unwrap());
                assert_ne!(
                    payload,
                    bind(&[LogicalType::BigInt])
                        .canonical_plan_payload()
                        .unwrap()
                );
                assert!(input_payloads.insert(payload));
            }
            if let Some(output) = function.copy_to {
                let bind =
                    |names: &[String]| (output.copy_to_bind)(&options, names, &types).unwrap();
                let payload = bind(&names).canonical_plan_payload();
                assert_eq!(payload, bind(&names).canonical_plan_payload());
                assert_ne!(payload, bind(&["b".into()]).canonical_plan_payload());
                assert!(output_payloads.insert(payload));
            }
        }
    }
}
