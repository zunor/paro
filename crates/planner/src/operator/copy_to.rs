// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use paro_common::types::LogicalType;
use paro_function::copy::{CopyFunction, CopyFunctionBindData, CopyOptions};
use paro_parser::ast::CopySource;

use crate::plan::LogicalPlan;

#[derive(Debug)]
pub struct CopyTo {
    pub copy_function: CopyFunction,
    pub bind_data: Arc<dyn CopyFunctionBindData>,
    pub file_path: String,
    pub source: CopySource,
    pub options: CopyOptions,
    pub child: Box<LogicalPlan>,
    pub names: Vec<String>,
    pub types: Vec<LogicalType>,
}

impl CopyTo {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        copy_function: CopyFunction,
        bind_data: Arc<dyn CopyFunctionBindData>,
        file_path: String,
        source: CopySource,
        options: CopyOptions,
        child: LogicalPlan,
        names: Vec<String>,
        types: Vec<LogicalType>,
    ) -> Self {
        Self {
            copy_function,
            bind_data,
            file_path,
            source,
            options,
            child: Box::new(child),
            names,
            types,
        }
    }
}
