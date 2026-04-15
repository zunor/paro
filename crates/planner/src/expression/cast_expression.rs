// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Bound Cast Expression
//!
//!

use super::Expression;
use paro_common::cast_rules::CastRules;
use paro_common::error::Result;
use paro_common::types::LogicalType;
use paro_function::scalar::cast::{BoundCastInfo, CastFunctionSet};

/// A bound cast expression.
#[derive(Debug, Clone)]
pub struct CastExpression {
    /// The expression to cast
    pub child: Box<Expression>,
    /// The target type to cast to
    pub target_type: LogicalType,
    /// If true, return NULL on cast failure instead of error (TRY_CAST)
    pub try_cast: bool,
    /// The bound cast implementation
    pub cast_info: BoundCastInfo,
}

impl CastExpression {
    /// Create a new cast expression.
    pub fn new(
        child: Expression,
        target_type: LogicalType,
        cast_info: BoundCastInfo,
        try_cast: bool,
    ) -> Self {
        Self {
            child: Box::new(child),
            target_type,
            try_cast,
            cast_info,
        }
    }

    /// Add a cast if needed (if source type != target type and implicit cast is allowed).
    pub fn add_cast_if_needed(
        expr: Expression,
        target_type: LogicalType,
        cast_functions: &CastFunctionSet,
    ) -> Result<Expression> {
        let source_type = expr.return_type();

        if source_type == target_type {
            return Ok(expr);
        }

        if let (LogicalType::Array(_, s_size), LogicalType::Array(_, t_size)) =
            (&source_type, &target_type)
        {
            if s_size != t_size {
                return Err(paro_common::error::type_mismatch(format!(
                    "Array size mismatch: cannot cast from {} to {}",
                    source_type, target_type
                )));
            }
        }

        let cost = CastRules::implicit_cast_cost(&expr.get_expression_return_type(), &target_type);
        if cost < 0 {
            return Err(paro_common::error::catalog(format!(
                "Cannot implicitly cast from {} to {}",
                source_type, target_type
            )));
        }

        let cast_info = cast_functions.get_cast_function(&source_type, &target_type)?;

        Ok(Expression::Cast(CastExpression::new(
            expr,
            target_type,
            cast_info,
            false,
        )))
    }

    /// Add an explicit CAST or TRY_CAST.
    pub fn add_explicit_cast(
        expr: Expression,
        target_type: LogicalType,
        cast_functions: &CastFunctionSet,
        try_cast: bool,
    ) -> Result<Expression> {
        let source_type = expr.return_type();

        if source_type == target_type {
            return Ok(expr);
        }

        if let (LogicalType::Array(_, s_size), LogicalType::Array(_, t_size)) =
            (&source_type, &target_type)
        {
            if s_size != t_size {
                return Err(paro_common::error::type_mismatch(format!(
                    "Array size mismatch: cannot cast from {} to {}",
                    source_type, target_type
                )));
            }
        }

        let cast_info = cast_functions.get_cast_function(&source_type, &target_type)?;

        Ok(Expression::Cast(CastExpression::new(
            expr,
            target_type,
            cast_info,
            try_cast,
        )))
    }
}
