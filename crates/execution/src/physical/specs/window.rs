// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use paro_common::types::LogicalType;
use paro_planner::expression::WindowExpression;

#[derive(Debug, Clone)]
pub struct WindowSpec {
    pub window_index: usize,
    pub expressions: Box<[WindowExpression]>,
    pub input_width: usize,
    pub output_names: Box<[String]>,
    pub output_types: Box<[LogicalType]>,
}
