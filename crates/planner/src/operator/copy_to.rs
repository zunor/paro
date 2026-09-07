// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use paro_common::types::LogicalType;
use paro_function::copy::{CopyFunctionBindData, CopyOptions, CopyToFunction};
use paro_parser::ast::CopySource;

use crate::plan::OwnedLogicalPlan;

#[derive(Debug, Clone)]
pub struct CopyTo<Child = Box<OwnedLogicalPlan>> {
    pub copy_function: CopyToFunction,
    pub bind_data: Arc<dyn CopyFunctionBindData>,
    pub file_path: String,
    pub source: CopySource,
    pub options: CopyOptions,
    pub child: Child,
    pub names: Vec<String>,
    pub types: Vec<LogicalType>,
}

impl CopyTo {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        copy_function: CopyToFunction,
        bind_data: Arc<dyn CopyFunctionBindData>,
        file_path: String,
        source: CopySource,
        options: CopyOptions,
        child: OwnedLogicalPlan,
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
