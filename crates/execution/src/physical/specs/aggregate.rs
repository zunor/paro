// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use paro_common::error::{self as paro_error, Result};
use paro_common::types::{LogicalType, PhysicalType};
use paro_function::aggregate::{AggregateComparison, AggregateFinalizeProjection};
use paro_function::scalar::function_data_equals;
use paro_planner::expression::{ComparisonType, Expression, OperatorType, ReferenceExpression};

/// Lossless physical representation used for a materialized group key.
///
/// Logical expressions and operator output retain their SQL types. Only the
/// rows owned by the aggregate operator use this representation, allowing
/// hashing and equality checks to operate on compact fixed-width values.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GroupKeyEncoding {
    Identity,
    PackedString {
        physical_type: LogicalType,
        max_length: usize,
    },
    OffsetInteger {
        physical_type: LogicalType,
        minimum: i128,
    },
}

#[derive(Debug, Clone)]
pub struct AggregateSpec {
    pub grouping_key_count: usize,
    /// Map each SQL-visible output column to the internal finalized state
    /// column. Empty means identity. A non-identity map lets correctness-proven
    /// functionally dependent GROUP BY values live as `first` states instead
    /// of participating in hash lookup, while preserving the logical schema.
    pub state_output_projection: Box<[usize]>,
    /// Estimated rows entering the aggregate before local parallelism.
    /// Runtime hash tables treat this as a bounded capacity hint, never as a
    /// correctness constraint.
    pub estimated_input_rows: Option<u64>,
    pub projection_exprs: Box<[Expression]>,
    pub payload_types: Box<[LogicalType]>,
    pub groups: Box<[Expression]>,
    pub group_key_encodings: Box<[GroupKeyEncoding]>,
    pub grouping_sets: Box<[Box<[usize]>]>,
    pub aggregates: Box<[Expression]>,
    pub grouping_functions: Box<[Box<[usize]>]>,
    pub aggregate_inputs: Box<[Box<[usize]>]>,
    pub aggregate_filters: Box<[Option<usize>]>,
    pub aggregate_orders: Box<[Box<[usize]>]>,
    /// Optional one-time reduction over finalized aggregate values. This is a
    /// hidden execution annotation and does not extend `output_names` or
    /// `output_types`.
    pub post_reduction: Option<PostAggregateReductionSpec>,
    /// HAVING predicate restricted to finalized aggregate outputs. Reference
    /// indices are rebased so column zero is the first aggregate value.
    pub having_filter: Box<[Expression]>,
    pub perfect_hash: Option<PerfectHashAggregatePlan>,
    pub output_names: Box<[String]>,
    pub output_types: Box<[LogicalType]>,
}

impl AggregateSpec {
    /// Validate the aggregate-owned post-reduction against the finalized
    /// aggregate domain that this physical operator actually produces.
    pub fn verify_post_reduction(&self) -> Result<()> {
        let Some(post) = &self.post_reduction else {
            return Ok(());
        };
        post.verify()?;
        let actual_types = self
            .aggregates
            .iter()
            .map(Expression::return_type)
            .collect::<Vec<_>>();
        if actual_types.as_slice() != post.aggregate_types.as_ref() {
            return Err(paro_error::internal(format!(
                "post-aggregate finalized domain mismatch: aggregate={actual_types:?} reduction={:?}",
                post.aggregate_types
            )));
        }
        if let Some(sources) = &post.input_rollup_sources {
            if self.aggregates.len() != 1 || post.reducers.len() != 1 || sources.len() != 1 {
                return Err(paro_error::internal(
                    "post-aggregate input rollup currently requires exactly one source aggregate and one reducer",
                ));
            }
            if !self.has_plain_grouping_domain() {
                return Err(paro_error::internal(
                    "post-aggregate input rollup requires one plain, non-empty grouping domain",
                ));
            }
            let Some(perfect_hash) = &self.perfect_hash else {
                return Err(paro_error::internal(
                    "post-aggregate input rollup requires a perfect aggregate plan",
                ));
            };
            if perfect_hash.max_local_tables <= 1 {
                return Err(paro_error::internal(
                    "post-aggregate input rollup requires multiple local perfect tables",
                ));
            }
            if !self.having_filter.is_empty() {
                return Err(paro_error::internal(
                    "post-aggregate input rollup cannot coexist with an ordinary HAVING filter",
                ));
            }
            let state_filter = post.state_filter_plan().ok_or_else(|| {
                paro_error::internal(
                    "post-aggregate input rollup requires a direct aggregate/scalar state predicate",
                )
            })?;
            for (reducer_idx, &source_idx) in sources.iter().enumerate() {
                let Some(Expression::Aggregate(source)) = self.aggregates.get(source_idx) else {
                    return Err(paro_error::internal(format!(
                        "post-aggregate input-rollup source {source_idx} for reducer {reducer_idx} is not an aggregate"
                    )));
                };
                let Some(Expression::Aggregate(reducer)) = post.reducers.get(reducer_idx) else {
                    return Err(paro_error::internal(format!(
                        "post-aggregate input-rollup reducer {reducer_idx} is not an aggregate"
                    )));
                };
                if source.is_distinct()
                    || source.filter.is_some()
                    || !source.order_bys.is_empty()
                    || self.aggregate_filters.get(source_idx) != Some(&None)
                    || !self
                        .aggregate_orders
                        .get(source_idx)
                        .is_some_and(|orders| orders.is_empty())
                    || source.function.destructor.is_some()
                    || !source.function.state_is_trivially_copyable()
                    || source.function.simple_update.is_none()
                    || source.function.state_filter.is_none()
                    || source.function.direct_state_filter.is_none()
                    || source.function.direct_update.is_none()
                    || source.return_type != reducer.return_type
                {
                    return Err(paro_error::internal(format!(
                        "post-aggregate input-rollup source {source_idx} is not a plain inline aggregate"
                    )));
                }
                if state_filter.aggregate_index != source_idx {
                    return Err(paro_error::internal(format!(
                        "post-aggregate input-rollup predicate targets aggregate {}, expected source {source_idx}",
                        state_filter.aggregate_index
                    )));
                }
                if reducer.is_distinct()
                    || reducer.filter.is_some()
                    || !reducer.order_bys.is_empty()
                    || reducer.function.destructor.is_some()
                {
                    return Err(paro_error::internal(format!(
                        "post-aggregate input-rollup reducer {reducer_idx} must be plain and inline"
                    )));
                }
                if !matches!(
                    reducer.children.as_slice(),
                    [Expression::Reference(reference)]
                        if reference.index == source_idx
                            && reference.return_type == source.return_type
                ) {
                    return Err(paro_error::internal(format!(
                        "post-aggregate input-rollup reducer {reducer_idx} must consume source {source_idx} directly"
                    )));
                }
                let expected_source = source.function.input_rollup_function().ok_or_else(|| {
                    paro_error::internal(format!(
                        "aggregate {} does not declare input-rollup semantics",
                        source.function.name
                    ))
                })?;
                let expected_reducer =
                    source.function.partial_merge_function().ok_or_else(|| {
                        paro_error::internal(format!(
                            "aggregate {} does not declare a finalized partial reducer",
                            source.function.name
                        ))
                    })?;
                if !expected_source.execution_semantics_equal(&source.function)
                    || !expected_reducer.execution_semantics_equal(&reducer.function)
                    || !function_data_equals(
                        source.bind_info.as_ref(),
                        source.function.bind_data.as_ref(),
                    )
                    || !function_data_equals(
                        reducer.bind_info.as_ref(),
                        reducer.function.bind_data.as_ref(),
                    )
                {
                    return Err(paro_error::internal(format!(
                        "post-aggregate input-rollup reducer {reducer_idx} does not match source {source_idx}'s declared law"
                    )));
                }
                let inputs = self.aggregate_inputs.get(source_idx).ok_or_else(|| {
                    paro_error::internal(format!(
                        "post-aggregate input-rollup source {source_idx} has no physical inputs"
                    ))
                })?;
                if inputs.len() != source.children.len()
                    || inputs.len() != source.function.arguments.len()
                {
                    return Err(paro_error::internal(format!(
                        "post-aggregate input-rollup source {source_idx} input count mismatch: physical={} expression={} function={}",
                        inputs.len(),
                        source.children.len(),
                        source.function.arguments.len()
                    )));
                }
                for (argument_idx, (&payload_idx, expected_type)) in inputs
                    .iter()
                    .zip(source.function.arguments.iter())
                    .enumerate()
                {
                    let actual_type = self.payload_types.get(payload_idx).ok_or_else(|| {
                        paro_error::internal(format!(
                            "post-aggregate input-rollup source {source_idx} argument {argument_idx} references missing payload column {payload_idx}"
                        ))
                    })?;
                    if actual_type != expected_type {
                        return Err(paro_error::internal(format!(
                            "post-aggregate input-rollup source {source_idx} argument {argument_idx} type mismatch: expected={expected_type:?} actual={actual_type:?}"
                        )));
                    }
                    if !matches!(
                        source.children.get(argument_idx),
                        Some(Expression::Reference(reference))
                            if reference.index == payload_idx
                                && &reference.return_type == expected_type
                    ) {
                        return Err(paro_error::internal(format!(
                            "post-aggregate input-rollup source {source_idx} argument {argument_idx} disagrees with payload mapping {payload_idx}"
                        )));
                    }
                }
            }
        }
        for (index, having) in self.having_filter.iter().enumerate() {
            if having.return_type() != LogicalType::Boolean {
                return Err(paro_error::internal(format!(
                    "aggregate HAVING expression {index} must return Boolean"
                )));
            }
            verify_local_expression(
                having,
                &post.aggregate_types,
                &format!("aggregate HAVING expression {index}"),
                None,
            )?;
        }
        Ok(())
    }

    pub(crate) fn has_plain_grouping_domain(&self) -> bool {
        self.grouping_key_count > 0
            && self.groups.len() == self.grouping_key_count
            && self.grouping_functions.is_empty()
            && (self.grouping_sets.is_empty()
                || (self.grouping_sets.len() == 1
                    && self.grouping_sets[0].len() == self.grouping_key_count
                    && self.grouping_sets[0]
                        .iter()
                        .copied()
                        .collect::<std::collections::HashSet<_>>()
                        == (0..self.grouping_key_count).collect()))
    }
}

/// Typed physical form of a post-aggregate scalar reduction.
///
/// Every expression domain is local and positional:
///
/// - reducer aggregate children reference `0..aggregates.len()`;
/// - scalar expressions reference `0..reducers.len()`;
/// - predicate references address original aggregate values first, followed by
///   scalar outputs (`0..aggregates.len() + scalar_expressions.len()`).
#[derive(Debug, Clone)]
pub struct PostAggregateReductionSpec {
    /// Finalized aggregate-value domain consumed by reducers and exposed to
    /// the dynamic predicate. Keeping this domain explicit lets the execution
    /// boundary reject a stale or hand-built physical spec before an unsafe
    /// vector kernel observes a mismatched physical representation.
    pub aggregate_types: Box<[LogicalType]>,
    pub reducers: Box<[Expression]>,
    pub reducer_types: Box<[LogicalType]>,
    pub scalar_expressions: Box<[Expression]>,
    pub scalar_types: Box<[LogicalType]>,
    pub predicate: Expression,
    /// Optional reducer-to-source mapping for reductions that can be folded
    /// directly over the grouped aggregate's original input. Every entry is
    /// an index into [`AggregateSpec::aggregates`]. This is an all-or-nothing
    /// execution optimization: absence retains the preserving finalized-group
    /// traversal, while presence must cover every reducer.
    pub input_rollup_sources: Option<Box<[usize]>>,
}

/// Static, value-independent shape of a post-reduction state predicate.
///
/// Physical planning uses this proof to distinguish a logical input-rollup
/// candidate from an executable perfect-table strategy. Runtime may later
/// pair the plan with the one-row scalar value without rediscovering cast,
/// reference-domain, or comparison-orientation semantics.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PostAggregateStateFilterPlan {
    pub aggregate_index: usize,
    pub scalar_index: usize,
    pub projection: AggregateFinalizeProjection,
    pub comparison: AggregateComparison,
}

impl PostAggregateReductionSpec {
    /// Recognize the exact predicate form supported by direct aggregate-state
    /// filtering: one finalized aggregate (optionally under the canonical
    /// DECIMAL cast) compared with one hidden scalar reference.
    pub fn state_filter_plan(&self) -> Option<PostAggregateStateFilterPlan> {
        let Expression::Comparison(comparison) = &self.predicate else {
            return None;
        };
        let aggregate_count = self.aggregate_types.len();
        let (aggregate, scalar, comparison) = match (&*comparison.left, &*comparison.right) {
            (aggregate, Expression::Reference(scalar)) if scalar.index >= aggregate_count => (
                compile_finalize_projection(aggregate, &self.aggregate_types)?,
                scalar,
                map_comparison(comparison.comparison_type)?,
            ),
            (Expression::Reference(scalar), aggregate) if scalar.index >= aggregate_count => (
                compile_finalize_projection(aggregate, &self.aggregate_types)?,
                scalar,
                map_comparison(invert_comparison(comparison.comparison_type)?)?,
            ),
            _ => return None,
        };
        let scalar_index = scalar.index.checked_sub(aggregate_count)?;
        if self.scalar_types.get(scalar_index) != Some(&scalar.return_type)
            || scalar.return_type != aggregate.projected_type
        {
            return None;
        }
        Some(PostAggregateStateFilterPlan {
            aggregate_index: aggregate.reference.index,
            scalar_index,
            projection: aggregate.projection,
            comparison,
        })
    }

    /// Verify every positional expression domain carried by the physical
    /// annotation. Logical verification proves the rewrite; this check guards
    /// the independently public physical representation at execution time.
    pub fn verify(&self) -> Result<()> {
        if self.aggregate_types.is_empty()
            || self.reducers.is_empty()
            || self.scalar_expressions.is_empty()
        {
            return Err(paro_error::internal(
                "post-aggregate reduction requires aggregate inputs, reducers, and scalar expressions",
            ));
        }
        if self.reducers.len() != self.reducer_types.len() {
            return Err(paro_error::internal(format!(
                "post-aggregate reducer descriptor mismatch: reducers={} types={}",
                self.reducers.len(),
                self.reducer_types.len()
            )));
        }
        if self
            .input_rollup_sources
            .as_ref()
            .is_some_and(|sources| sources.len() != self.reducers.len())
        {
            return Err(paro_error::internal(format!(
                "post-aggregate input-rollup descriptor mismatch: sources={} reducers={}",
                self.input_rollup_sources
                    .as_deref()
                    .map_or(0, <[usize]>::len),
                self.reducers.len()
            )));
        }
        for (reducer_idx, (expression, expected_type)) in self
            .reducers
            .iter()
            .zip(self.reducer_types.iter())
            .enumerate()
        {
            let Expression::Aggregate(reducer) = expression else {
                return Err(paro_error::internal(format!(
                    "post-aggregate reducer {reducer_idx} is not an aggregate expression"
                )));
            };
            if reducer.is_distinct()
                || reducer.filter.is_some()
                || !reducer.order_bys.is_empty()
                || reducer.function.destructor.is_some()
            {
                return Err(paro_error::internal(format!(
                    "post-aggregate reducer {reducer_idx} must be non-distinct, unfiltered, unordered, and inline-owned"
                )));
            }
            if reducer.return_type != reducer.function.return_type
                || expression.return_type() != *expected_type
            {
                return Err(paro_error::internal(format!(
                    "post-aggregate reducer type mismatch at {reducer_idx}: function={:?} expression={:?} spec={expected_type:?}",
                    reducer.function.return_type,
                    reducer.return_type
                )));
            }
            if reducer.children.len() != reducer.function.arguments.len() {
                return Err(paro_error::internal(format!(
                    "post-aggregate reducer {reducer_idx} argument count mismatch: function={} expressions={}",
                    reducer.function.arguments.len(),
                    reducer.children.len()
                )));
            }
            for (argument_idx, child) in reducer.children.iter().enumerate() {
                let Expression::Reference(reference) = child else {
                    return Err(paro_error::internal(format!(
                        "post-aggregate reducer {reducer_idx} argument {argument_idx} must directly reference a finalized aggregate value"
                    )));
                };
                if reducer.function.arguments.get(argument_idx) != Some(&reference.return_type) {
                    return Err(paro_error::internal(format!(
                        "post-aggregate reducer {reducer_idx} argument {argument_idx} disagrees with its bound function"
                    )));
                }
                verify_local_expression(
                    child,
                    &self.aggregate_types,
                    &format!("post-aggregate reducer {reducer_idx} argument {argument_idx}"),
                    None,
                )?;
            }
        }

        if self.scalar_expressions.len() != self.scalar_types.len() {
            return Err(paro_error::internal(format!(
                "post-aggregate scalar descriptor mismatch: expressions={} types={}",
                self.scalar_expressions.len(),
                self.scalar_types.len()
            )));
        }
        for (scalar_idx, (expression, expected_type)) in self
            .scalar_expressions
            .iter()
            .zip(self.scalar_types.iter())
            .enumerate()
        {
            if expression.return_type() != *expected_type {
                return Err(paro_error::internal(format!(
                    "post-aggregate scalar type mismatch at {scalar_idx}: expression={:?} spec={expected_type:?}",
                    expression.return_type()
                )));
            }
            verify_local_expression(
                expression,
                &self.reducer_types,
                &format!("post-aggregate scalar expression {scalar_idx}"),
                None,
            )?;
        }

        if self.predicate.return_type() != LogicalType::Boolean {
            return Err(paro_error::internal(
                "post-aggregate predicate must return Boolean",
            ));
        }
        let predicate_types = self
            .aggregate_types
            .iter()
            .cloned()
            .chain(self.scalar_types.iter().cloned())
            .collect::<Vec<_>>();
        verify_local_expression(
            &self.predicate,
            &predicate_types,
            "post-aggregate predicate",
            Some(self.aggregate_types.len()),
        )?;
        Ok(())
    }
}

struct CompiledFinalizeProjection<'a> {
    reference: &'a ReferenceExpression,
    projection: AggregateFinalizeProjection,
    projected_type: LogicalType,
}

fn compile_finalize_projection<'a>(
    expression: &'a Expression,
    aggregate_types: &[LogicalType],
) -> Option<CompiledFinalizeProjection<'a>> {
    match expression {
        Expression::Reference(reference)
            if aggregate_types.get(reference.index) == Some(&reference.return_type) =>
        {
            Some(CompiledFinalizeProjection {
                reference,
                projection: AggregateFinalizeProjection::Identity,
                projected_type: reference.return_type.clone(),
            })
        }
        Expression::Cast(cast) => {
            let Expression::Reference(reference) = cast.child.as_ref() else {
                return None;
            };
            if aggregate_types.get(reference.index) != Some(&reference.return_type)
                || !matches!(reference.return_type, LogicalType::Decimal { .. })
                || !cast.cast_info.is_canonical_decimal_cast()
            {
                return None;
            }
            let LogicalType::Decimal { precision, scale } = cast.target_type else {
                return None;
            };
            Some(CompiledFinalizeProjection {
                reference,
                projection: AggregateFinalizeProjection::DecimalCast {
                    target_precision: precision,
                    target_scale: scale,
                    try_cast: cast.try_cast,
                },
                projected_type: cast.target_type.clone(),
            })
        }
        _ => None,
    }
}

fn invert_comparison(comparison: ComparisonType) -> Option<ComparisonType> {
    Some(match comparison {
        ComparisonType::Equal => ComparisonType::Equal,
        ComparisonType::NotEqual => ComparisonType::NotEqual,
        ComparisonType::LessThan => ComparisonType::GreaterThan,
        ComparisonType::LessThanOrEqual => ComparisonType::GreaterThanOrEqual,
        ComparisonType::GreaterThan => ComparisonType::LessThan,
        ComparisonType::GreaterThanOrEqual => ComparisonType::LessThanOrEqual,
        ComparisonType::DistinctFrom | ComparisonType::NotDistinctFrom => return None,
    })
}

fn map_comparison(comparison: ComparisonType) -> Option<AggregateComparison> {
    Some(match comparison {
        ComparisonType::Equal => AggregateComparison::Equal,
        ComparisonType::NotEqual => AggregateComparison::NotEqual,
        ComparisonType::LessThan => AggregateComparison::LessThan,
        ComparisonType::LessThanOrEqual => AggregateComparison::LessThanOrEqual,
        ComparisonType::GreaterThan => AggregateComparison::GreaterThan,
        ComparisonType::GreaterThanOrEqual => AggregateComparison::GreaterThanOrEqual,
        ComparisonType::DistinctFrom | ComparisonType::NotDistinctFrom => return None,
    })
}

pub(crate) fn verify_local_expression(
    expression: &Expression,
    domain: &[LogicalType],
    label: &str,
    hidden_start: Option<usize>,
) -> Result<()> {
    let properties = expression.evaluation_properties();
    if !properties.can_share_evaluation() || properties.is_reorder_fence() {
        return Err(paro_error::internal(format!(
            "{label} must be deterministic, side-effect free, and native"
        )));
    }

    let mut saw_hidden_reference = false;
    verify_local_expression_node(
        expression,
        domain,
        label,
        hidden_start,
        &mut saw_hidden_reference,
    )?;
    if hidden_start.is_some() && !saw_hidden_reference {
        return Err(paro_error::internal(format!(
            "{label} must reference a hidden reduction value"
        )));
    }
    Ok(())
}

fn verify_local_expression_node(
    expression: &Expression,
    domain: &[LogicalType],
    label: &str,
    hidden_start: Option<usize>,
    saw_hidden_reference: &mut bool,
) -> Result<()> {
    match expression {
        Expression::Constant(constant) => {
            let value_type = constant.value.logical_type();
            let compatible = value_type == constant.return_type
                || matches!(
                    (&constant.value, &constant.return_type),
                    (
                        paro_common::runtime_value::Value::Varchar(_),
                        LogicalType::VarcharCollation(_)
                            | LogicalType::TsVector
                            | LogicalType::TsQuery
                            | LogicalType::Json
                            | LogicalType::Jsonb
                    )
                );
            if !compatible {
                return Err(paro_error::internal(format!(
                    "{label} constant type mismatch: value={value_type:?} expression={:?}",
                    constant.return_type
                )));
            }
        }
        Expression::Reference(reference) => match domain.get(reference.index) {
            Some(expected_type) if expected_type == &reference.return_type => {
                if hidden_start.is_some_and(|start| reference.index >= start) {
                    *saw_hidden_reference = true;
                }
            }
            Some(expected_type) => {
                return Err(paro_error::internal(format!(
                    "{label} reference {} type mismatch: expected={expected_type:?} actual={:?}",
                    reference.index, reference.return_type
                )));
            }
            None => {
                return Err(paro_error::internal(format!(
                    "{label} reference {} is out of bounds for domain width {}",
                    reference.index,
                    domain.len()
                )));
            }
        },
        Expression::Function(function) => {
            if function.return_type != function.function.return_type {
                return Err(paro_error::internal(format!(
                    "{label} scalar function {} return type mismatch: function={:?} expression={:?}",
                    function.function.name,
                    function.function.return_type,
                    function.return_type
                )));
            }
            let fixed_count = function.function.arguments.len();
            let valid_arity = if function.function.varargs.is_some() {
                function.children.len() >= fixed_count
            } else {
                function.children.len() == fixed_count
            };
            if !valid_arity {
                return Err(paro_error::internal(format!(
                    "{label} scalar function {} argument count mismatch: fixed={fixed_count} actual={}",
                    function.function.name,
                    function.children.len()
                )));
            }
            for (index, child) in function.children.iter().enumerate() {
                let expected = function
                    .function
                    .arguments
                    .get(index)
                    .or(function.function.varargs.as_ref())
                    .ok_or_else(|| {
                        paro_error::internal(format!(
                            "{label} scalar function {} has no contract for argument {index}",
                            function.function.name
                        ))
                    })?;
                if child.return_type() != *expected {
                    return Err(paro_error::internal(format!(
                        "{label} scalar function {} argument {index} type mismatch: expected={expected:?} actual={:?}",
                        function.function.name,
                        child.return_type()
                    )));
                }
                verify_local_expression_node(
                    child,
                    domain,
                    label,
                    hidden_start,
                    saw_hidden_reference,
                )?;
            }
        }
        Expression::Cast(cast) => {
            let source_type = cast.child.return_type();
            match cast.cast_info.type_contract() {
                Some((source, target)) if source == &source_type && target == &cast.target_type => {
                }
                Some((source, target)) => {
                    return Err(paro_error::internal(format!(
                        "{label} cast contract mismatch: dispatch={source:?}->{target:?} expression={source_type:?}->{:?}",
                        cast.target_type
                    )));
                }
                None => {
                    return Err(paro_error::internal(format!(
                        "{label} cast has no bound source/target contract"
                    )));
                }
            }
            verify_local_expression_node(
                &cast.child,
                domain,
                label,
                hidden_start,
                saw_hidden_reference,
            )?;
        }
        Expression::Comparison(comparison) => {
            if comparison.left.return_type() != comparison.right.return_type() {
                return Err(paro_error::internal(format!(
                    "{label} comparison operand type mismatch: left={:?} right={:?}",
                    comparison.left.return_type(),
                    comparison.right.return_type()
                )));
            }
            verify_local_expression_node(
                &comparison.left,
                domain,
                label,
                hidden_start,
                saw_hidden_reference,
            )?;
            verify_local_expression_node(
                &comparison.right,
                domain,
                label,
                hidden_start,
                saw_hidden_reference,
            )?;
        }
        Expression::Conjunction(conjunction) => {
            if conjunction.children.is_empty()
                || conjunction
                    .children
                    .iter()
                    .any(|child| child.return_type() != LogicalType::Boolean)
            {
                return Err(paro_error::internal(format!(
                    "{label} conjunction requires non-empty Boolean children"
                )));
            }
            for child in &conjunction.children {
                verify_local_expression_node(
                    child,
                    domain,
                    label,
                    hidden_start,
                    saw_hidden_reference,
                )?;
            }
        }
        Expression::Case(case) => {
            if case.check.return_type() != LogicalType::Boolean
                || case.result_if_true.return_type() != case.return_type
                || case.result_if_false.return_type() != case.return_type
            {
                return Err(paro_error::internal(format!(
                    "{label} CASE expression has inconsistent bound types"
                )));
            }
            for child in [
                case.check.as_ref(),
                case.result_if_true.as_ref(),
                case.result_if_false.as_ref(),
            ] {
                verify_local_expression_node(
                    child,
                    domain,
                    label,
                    hidden_start,
                    saw_hidden_reference,
                )?;
            }
        }
        Expression::Operator(operator) => {
            verify_operator_types(operator, label)?;
            for child in &operator.children {
                verify_local_expression_node(
                    child,
                    domain,
                    label,
                    hidden_start,
                    saw_hidden_reference,
                )?;
            }
        }
        Expression::Parameter(_) => {}
        Expression::ColumnRef(_)
        | Expression::Aggregate(_)
        | Expression::Subquery(_)
        | Expression::Window(_) => {
            return Err(paro_error::internal(format!(
                "{label} contains an invalid physical expression domain"
            )));
        }
    }
    Ok(())
}

fn verify_operator_types(
    operator: &paro_planner::expression::OperatorExpression,
    label: &str,
) -> Result<()> {
    let types = operator
        .children
        .iter()
        .map(Expression::return_type)
        .collect::<Vec<_>>();
    let valid = match operator.operator_type {
        OperatorType::Not => {
            operator.return_type == LogicalType::Boolean
                && types.as_slice() == [LogicalType::Boolean]
        }
        OperatorType::IsNull | OperatorType::IsNotNull => {
            operator.return_type == LogicalType::Boolean && types.len() == 1
        }
        OperatorType::Coalesce => {
            !types.is_empty() && types.iter().all(|ty| ty == &operator.return_type)
        }
        OperatorType::In | OperatorType::NotIn => {
            operator.return_type == LogicalType::Boolean
                && types.len() >= 2
                && types[1..].iter().all(|ty| ty == &types[0])
        }
        OperatorType::Like | OperatorType::ILike => {
            operator.return_type == LogicalType::Boolean
                && types.len() == 2
                && types
                    .iter()
                    .all(|ty| ty.physical_type() == PhysicalType::Varchar)
        }
        OperatorType::ArrayConstructor => match &operator.return_type {
            LogicalType::Array(element, size) => {
                *size == types.len() && types.iter().all(|ty| ty == element.as_ref())
            }
            _ => false,
        },
        OperatorType::StructConstructor => match &operator.return_type {
            LogicalType::Struct(fields) => {
                fields.len() == types.len()
                    && fields
                        .iter()
                        .zip(&types)
                        .all(|((_, expected), actual)| expected == actual)
            }
            _ => false,
        },
        OperatorType::ErrorIfMultipleRows => {
            types.len() == 2 && types[0] == operator.return_type && types[1] == LogicalType::BigInt
        }
        // No physical executor exists for this operator today.
        OperatorType::ArrayExtract => false,
    };
    if !valid {
        return Err(paro_error::internal(format!(
            "{label} has an invalid {:?} operator contract: children={types:?} return={:?}",
            operator.operator_type, operator.return_type
        )));
    }
    Ok(())
}

#[derive(Debug, Clone)]
pub struct PerfectHashAggregatePlan {
    pub group_minima: Box<[i128]>,
    pub group_cardinalities: Box<[usize]>,
    /// Maximum number of concurrent local direct-addressing tables admitted by
    /// the aggregate memory budget.
    pub max_local_tables: usize,
}
