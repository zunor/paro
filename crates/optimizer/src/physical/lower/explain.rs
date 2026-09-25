// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::*;

pub(crate) fn explain_line_expression(line: impl Into<String>) -> Box<[Expression]> {
    Box::new([Expression::Constant(
        ConstantExpression::new(
            Value::Varchar(line.into()),
            paro_common::types::LogicalType::Varchar,
        )
        .into(),
    )])
}
