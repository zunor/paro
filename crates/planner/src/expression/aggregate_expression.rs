// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Bound Aggregate Expression
//!
//!

use super::{Expression, OrderByExpression};
use paro_common::types::LogicalType;
use paro_function::aggregate::{AggregateFunction, FunctionData};
use std::sync::Arc;

/// Aggregate DISTINCT modifier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum AggregateType {
    /// Regular aggregate without DISTINCT.
    #[default]
    NonDistinct,
    /// DISTINCT aggregate.
    Distinct,
}

/// A bound aggregate function.
#[derive(Debug, Clone)]
pub struct AggregateExpression {
    /// The aggregate function logic.
    pub function: AggregateFunction,
    /// Arguments to the aggregate function.
    pub children: Vec<Expression>,
    /// The resolved return type.
    pub return_type: LogicalType,
    /// Aggregate DISTINCT modifier.
    pub aggr_type: AggregateType,
    /// Optional FILTER (WHERE...) expression.
    pub filter: Option<Box<Expression>>,
    /// Optional WITHIN GROUP / ordered aggregate expressions.
    pub order_bys: Vec<OrderByExpression>,
    pub bind_info: Option<Arc<dyn FunctionData>>,
}

impl AggregateExpression {
    pub fn new(
        function: AggregateFunction,
        children: Vec<Expression>,
        return_type: LogicalType,
    ) -> Self {
        Self {
            bind_info: function.bind_data.clone(),
            function,
            children,
            return_type,
            aggr_type: AggregateType::NonDistinct,
            filter: None,
            order_bys: Vec::new(),
        }
    }

    pub fn with_aggr_type(mut self, aggr_type: AggregateType) -> Self {
        self.aggr_type = aggr_type;
        self
    }

    pub fn is_distinct(&self) -> bool {
        self.aggr_type == AggregateType::Distinct
    }

    pub fn with_filter(mut self, filter: Option<Expression>) -> Self {
        self.filter = filter.map(Box::new);
        self
    }

    pub fn with_order_bys(mut self, order_bys: Vec<OrderByExpression>) -> Self {
        self.order_bys = order_bys;
        self
    }

    pub fn with_bind_info(mut self, bind_info: Option<Arc<dyn FunctionData>>) -> Self {
        self.bind_info = bind_info;
        self
    }
}
