// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use paro_common::types::LogicalType;

use super::driver::SearchOperatorDriver;

#[derive(Debug)]
pub struct SearchSourceGlobal {
    pub output_types: Box<[LogicalType]>,
}

#[derive(Debug, Default)]
pub struct SearchSourceLocal {
    pub(crate) driver: Option<SearchOperatorDriver>,
}
