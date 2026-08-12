// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use paro_common::types::LogicalType;
use paro_function::window::WindowFunction;
use paro_planner::expression::{WindowExpression, WindowFrame};

use crate::physical::specs::WindowSpec;

pub(super) fn window_spec() -> WindowSpec {
    WindowSpec {
        window_index: 1,
        expressions: vec![WindowExpression::native(
            WindowFunction::row_number(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            WindowFrame::default(),
            false,
        )]
        .into_boxed_slice(),
        input_width: 1,
        output_names: Box::new(["a".to_string(), "rn".to_string()]),
        output_types: Box::new([LogicalType::Integer, LogicalType::BigInt]),
    }
}
