// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use paro_common::error::{self as paro_error, Result};
use paro_common::types::LogicalType;
use paro_planner::expression::{AggregateType, Expression, WindowExpression};

use super::{AggregateSpec, GroupKeyEncoding};

#[derive(Debug, Clone)]
pub struct WindowSpec {
    pub window_index: usize,
    pub expressions: Box<[WindowExpression]>,
    pub input_width: usize,
    pub output_names: Box<[String]>,
    pub output_types: Box<[LogicalType]>,
}

/// Full-partition aggregate window executed without sorting.
///
/// The operator preserves selected detail columns while building one grouped
/// aggregate over the same input. After every group has finalized, emit tasks
/// replay detail batches and attach the immutable aggregate result addressed
/// by each row's partition key. This is the physical shape for aggregate
/// windows whose frame is the complete partition and whose result therefore
/// does not depend on row order.
#[derive(Debug, Clone)]
pub struct PartitionAggregateWindowSpec {
    pub domain: PartitionAggregateDomain,
    /// Types presented to the breaker sink before aggregate projection.
    pub input_types: Box<[LogicalType]>,
    /// Input columns retained in the detail stream, in output order.
    pub detail_columns: Box<[usize]>,
    /// Grouped aggregate plan. Group references address its projected payload.
    pub aggregate: AggregateSpec,
    /// Detail columns followed by finalized aggregate columns.
    pub output_names: Box<[String]>,
    pub output_types: Box<[LogicalType]>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PartitionAggregateDomain {
    Global,
    Keyed,
}

impl PartitionAggregateWindowSpec {
    /// Verify the deliberately narrow first execution contract.
    ///
    /// Unsupported aggregate modifiers are rejected at the physical boundary
    /// rather than silently taking a path that cannot preserve their state.
    /// The representation remains generic over aggregate functions. Its first
    /// lookup backend deliberately admits one INTEGER key; other SQL key
    /// domains need the aggregate tuple codec/equality contract before they can
    /// extend this protocol safely. Future key, DISTINCT, and ordered backends
    /// do not need to change detail/index publication.
    pub fn verify(&self) -> Result<()> {
        let aggregate = &self.aggregate;
        if aggregate.groups.len() != aggregate.grouping_key_count || aggregate.aggregates.is_empty()
        {
            return Err(paro_error::internal(
                "partition aggregate window requires groups and aggregate functions",
            ));
        }
        match self.domain {
            PartitionAggregateDomain::Global
                if aggregate.grouping_key_count != 0 || !aggregate.groups.is_empty() =>
            {
                return Err(paro_error::internal(
                    "global aggregate window cannot carry partition keys",
                ));
            }
            PartitionAggregateDomain::Keyed
                if aggregate.groups.len() != 1
                    || !matches!(
                        aggregate.groups[0].return_type(),
                        LogicalType::Integer | LogicalType::BigInt
                    ) =>
            {
                return Err(paro_error::not_implemented(
                    "partition aggregate window's keyed lookup backend requires one INTEGER or BIGINT partition key",
                ));
            }
            _ => {}
        }
        if !aggregate.grouping_functions.is_empty()
            || !aggregate.state_output_projection.is_empty()
            || !aggregate.having_filter.is_empty()
            || aggregate.post_reduction.is_some()
            || aggregate.perfect_hash.is_some()
        {
            return Err(paro_error::not_implemented(
                "partition aggregate window does not support grouping functions, HAVING, post-reduction, state projection, or perfect-hash metadata",
            ));
        }
        if aggregate.group_key_encodings.len() != aggregate.grouping_key_count
            || aggregate
                .group_key_encodings
                .iter()
                .any(|encoding| !matches!(encoding, GroupKeyEncoding::Identity))
        {
            return Err(paro_error::not_implemented(
                "partition aggregate window currently requires identity group-key encoding",
            ));
        }
        if !aggregate.grouping_sets.is_empty()
            && (aggregate.grouping_sets.len() != 1
                || aggregate.grouping_sets[0].as_ref()
                    != (0..aggregate.grouping_key_count)
                        .collect::<Vec<_>>()
                        .as_slice())
        {
            return Err(paro_error::not_implemented(
                "partition aggregate window requires one complete grouping set",
            ));
        }
        if aggregate
            .aggregates
            .iter()
            .enumerate()
            .any(|(idx, expression)| {
                !matches!(expression, Expression::Aggregate(bound)
                    if bound.aggr_type == AggregateType::NonDistinct
                        && bound.function.destructor.is_none()
                        && bound.order_bys.is_empty()
                        && aggregate.aggregate_orders.get(idx).is_some_and(|orders| orders.is_empty()))
            })
        {
            return Err(paro_error::not_implemented(
                "partition aggregate window currently supports plain, unordered aggregates with destructor-free states",
            ));
        }
        if aggregate.aggregate_inputs.len() != aggregate.aggregates.len()
            || aggregate.aggregate_filters.len() != aggregate.aggregates.len()
            || aggregate.aggregate_orders.len() != aggregate.aggregates.len()
        {
            return Err(paro_error::internal(
                "partition aggregate window aggregate descriptors are misaligned",
            ));
        }
        if aggregate.projection_exprs.len() != aggregate.payload_types.len()
            || aggregate
                .projection_exprs
                .iter()
                .zip(aggregate.payload_types.iter())
                .any(|(expression, ty)| expression.return_type() != *ty)
        {
            return Err(paro_error::internal(
                "partition aggregate window projected payload schema is inconsistent",
            ));
        }
        if aggregate.projection_exprs.len() < self.input_types.len()
            || aggregate.payload_types.len() < self.input_types.len()
        {
            return Err(paro_error::internal(
                "partition aggregate payload is missing its stable input prefix",
            ));
        }
        for (index, input_type) in self.input_types.iter().enumerate() {
            if aggregate.payload_types.get(index) != Some(input_type)
                || !matches!(aggregate.projection_exprs.get(index),
                    Some(Expression::Reference(reference))
                        if reference.index == index && reference.return_type == *input_type)
            {
                return Err(paro_error::internal(format!(
                    "partition aggregate payload prefix column {index} is not the corresponding input reference"
                )));
            }
        }
        for (index, group) in aggregate.groups.iter().enumerate() {
            let Expression::Reference(reference) = group else {
                return Err(paro_error::internal(format!(
                    "partition key {index} is not a payload reference"
                )));
            };
            if aggregate.payload_types.get(reference.index) != Some(&reference.return_type) {
                return Err(paro_error::internal(format!(
                    "partition key {index} references an invalid payload column"
                )));
            }
        }
        for (index, inputs) in aggregate.aggregate_inputs.iter().enumerate() {
            let Expression::Aggregate(bound) = &aggregate.aggregates[index] else {
                return Err(paro_error::internal(format!(
                    "partition aggregate {index} lost its aggregate expression"
                )));
            };
            if inputs
                .iter()
                .any(|input| *input >= aggregate.payload_types.len())
            {
                return Err(paro_error::internal(format!(
                    "partition aggregate {index} references an invalid payload column"
                )));
            }
            if aggregate.aggregate_filters[index]
                .is_some_and(|filter| filter >= aggregate.payload_types.len())
            {
                return Err(paro_error::internal(format!(
                    "partition aggregate {index} filter references an invalid payload column"
                )));
            }
            if bound.children.len() != inputs.len()
                || bound.children.iter().zip(inputs).any(|(child, input)| {
                    !matches!(child, Expression::Reference(reference)
                            if reference.index == *input
                                && aggregate.payload_types.get(*input)
                                    == Some(&reference.return_type))
                })
            {
                return Err(paro_error::internal(format!(
                    "partition aggregate {index} input expression and descriptor disagree"
                )));
            }
            match (&bound.filter, aggregate.aggregate_filters[index]) {
                (None, None) => {}
                (Some(filter), Some(filter_index))
                    if matches!(filter.as_ref(), Expression::Reference(reference)
                        if reference.index == filter_index
                            && reference.return_type == LogicalType::Boolean
                            && aggregate.payload_types.get(filter_index)
                                == Some(&LogicalType::Boolean)) => {}
                _ => {
                    return Err(paro_error::internal(format!(
                        "partition aggregate {index} FILTER expression and descriptor disagree"
                    )));
                }
            }
        }
        let expected_aggregate_types = aggregate
            .groups
            .iter()
            .chain(aggregate.aggregates.iter())
            .map(Expression::return_type)
            .collect::<Vec<_>>();
        if aggregate.output_names.len() != expected_aggregate_types.len()
            || aggregate.output_types.as_ref() != expected_aggregate_types.as_slice()
        {
            return Err(paro_error::internal(
                "partition aggregate internal finalized schema is inconsistent",
            ));
        }
        let mut expected_types =
            Vec::with_capacity(self.detail_columns.len() + aggregate.aggregates.len());
        for (index, &column) in self.detail_columns.iter().enumerate() {
            expected_types.push(self.input_types.get(column).cloned().ok_or_else(|| {
                paro_error::internal(format!(
                    "partition aggregate detail column {index} is out of bounds: {column}"
                ))
            })?);
        }
        expected_types.extend(aggregate.aggregates.iter().map(Expression::return_type));
        if expected_types.as_slice() != self.output_types.as_ref()
            || self.output_names.len() != self.output_types.len()
        {
            return Err(paro_error::internal(format!(
                "partition aggregate output schema mismatch: expected={expected_types:?}, actual={:?}",
                self.output_types
            )));
        }
        Ok(())
    }

    #[inline]
    pub fn detail_column_count(&self) -> usize {
        self.detail_columns.len()
    }

    #[inline]
    pub fn aggregate_column_count(&self) -> usize {
        self.aggregate.aggregates.len()
    }
}
