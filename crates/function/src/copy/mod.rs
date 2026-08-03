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

/// Internal options for CopyFunction, produced during the bind phase.
pub trait CopyFunctionBindData: Send + Sync + Debug {
    fn as_any(&self) -> &dyn Any;
}

pub trait CopyToGlobalState: Send + Sync {
    fn as_any(&self) -> &dyn Any;
    fn as_any_mut(&mut self) -> &mut dyn Any;
}

pub trait CopyToLocalState: Send + Sync {
    fn as_any(&self) -> &dyn Any;
    fn as_any_mut(&mut self) -> &mut dyn Any;
}

/// Format-driven CopyFunction.
#[derive(Clone)]
pub struct CopyFunction {
    pub name: String,

    // COPY TO callback
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

    // COPY FROM: Corresponding TableFunction
    pub copy_from_bind: fn(
        CopyFromSource,
        &CopyOptions,
        &[String],
        &[LogicalType],
    ) -> Result<Box<dyn TableFunctionBindData>>,
    pub copy_from_function: TableFunction,

    pub extension: String,
}

impl std::fmt::Debug for CopyFunction {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CopyFunction")
            .field("name", &self.name)
            .field("extension", &self.extension)
            .finish()
    }
}

pub fn register_copy_functions() -> Vec<CopyFunction> {
    let mut functions = csv::register_copy_functions();
    functions.extend(json::register_copy_functions());
    functions
}
