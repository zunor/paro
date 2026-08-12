// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Runtime support for reductions over finalized grouped-aggregate values.

use std::sync::Arc;

use paro_common::allocator::{ArenaAllocator, MemoryTag};
use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::types::LogicalType;
use paro_common::vector::{SelectionVector, Vector, VECTOR_SIZE};
use paro_function::aggregate::{AggregateCombineType, AggregateInputData};
use paro_function::scalar::{function_data_equals, FunctionExecContext};
use paro_planner::expression::{ConjunctionExpression, ConjunctionType, Expression};

use crate::expression_executor::executor::{ExpressionExecutor, VectorKernelInput};
use crate::operators::aggregate::aggregate_kernel::{
    combine_states, finalize_states, update_states, AggregatePayload,
};
use crate::operators::aggregate::aggregate_object::{create_aggregate_objects, AggregateObject};
use crate::operators::aggregate::aggregate_state::AggregateStateLayout;
use crate::operators::aggregate::build_helpers::{
    fill_repeated_state_addresses, initialize_state_buffer, state_buffer_words,
};
use crate::operators::aggregate::grouped_aggregate_data::reference_index;
use crate::physical::specs::{AggregateSpec, PostAggregateReductionSpec};
use crate::runtime::breaker::single_state_addresses;
use crate::runtime::context::QueryRuntimeContext;
use crate::runtime::ExpressionEvalInput;

/// One ungrouped reducer state fed by finalized values from every group.
///
/// The grouped table remains the owner of its states. This object owns only
/// the independent reducer states and can therefore be destroyed immediately
/// after the scalar expressions have been evaluated.
pub(crate) struct PostAggregateReducer {
    objects: Vec<AggregateObject>,
    inputs: Vec<Vec<usize>>,
    input_types: Vec<Vec<LogicalType>>,
    aggregate_types: Box<[LogicalType]>,
    state_buffer: Vec<u64>,
    addresses: Vector,
    arena: ArenaAllocator,
    reducer_types: Box<[LogicalType]>,
    scalar_types: Box<[LogicalType]>,
    scalar_executor: ExpressionExecutor,
}

/// Task-local raw-input fold for reducers whose aggregate implementation
/// explicitly declares a homomorphism from grouped partials back to the
/// original input domain.
///
/// Unlike [`PostAggregateReducer`], this state is updated beside the grouped
/// table. Merging therefore costs O(tasks) and makes the hidden scalar
/// available before a perfect-table slot merge begins. Functions must opt in
/// explicitly; ordinary reducers retain the preserving finalized-group scan.
#[derive(Debug)]
pub(crate) struct PostAggregateInputRollup {
    source_indices: Box<[usize]>,
    objects: Vec<AggregateObject>,
    inputs: Vec<Vec<usize>>,
    layout: AggregateStateLayout,
    state_buffer: Vec<u64>,
    addresses: Vector,
    arena: ArenaAllocator,
    reducer_types: Box<[LogicalType]>,
    scalar_types: Box<[LogicalType]>,
    scalar_executor: ExpressionExecutor,
}

impl PostAggregateInputRollup {
    pub(crate) fn try_new(
        aggregate: &AggregateSpec,
        query: &QueryRuntimeContext,
    ) -> Result<Option<Self>> {
        aggregate.verify_post_reduction()?;
        let Some(post) = aggregate.post_reduction.as_ref() else {
            return Ok(None);
        };
        let Some(source_indices) = post.input_rollup_sources.as_ref() else {
            return Ok(None);
        };
        let source_expressions = source_indices
            .iter()
            .map(|&index| {
                aggregate.aggregates.get(index).cloned().ok_or_else(|| {
                    paro_error::internal(format!(
                        "post-aggregate input-rollup source index out of bounds: {index}"
                    ))
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let objects = create_aggregate_objects(&source_expressions)?;
        if objects.iter().any(|object| {
            object.function.destructor.is_some() || !object.function.state_is_trivially_copyable()
        }) {
            return Err(paro_error::internal(
                "post-aggregate input rollup requires inline, trivially copyable source states",
            ));
        }
        if objects.len() != 1 || objects[0].function.simple_update.is_none() {
            return Err(paro_error::internal(
                "post-aggregate input rollup requires one aggregate with a single-state update hook",
            ));
        }
        let inputs = source_indices
            .iter()
            .map(|&index| {
                aggregate
                    .aggregate_inputs
                    .get(index)
                    .map(|inputs| inputs.to_vec())
                    .ok_or_else(|| {
                        paro_error::internal(format!(
                            "post-aggregate input-rollup source {index} has no input descriptor"
                        ))
                    })
            })
            .collect::<Result<Vec<_>>>()?;
        let layout = AggregateStateLayout::new(&objects)?;
        let allocator = query.allocator(MemoryTag::HashTable);
        let mut state_buffer = vec![0u64; state_buffer_words(layout.total_size())];
        initialize_state_buffer(&layout, &objects, &mut state_buffer, allocator.clone())?;
        let addresses =
            single_state_addresses(state_buffer.as_mut_ptr().cast::<u8>(), allocator.clone())?;
        Ok(Some(Self {
            source_indices: source_indices.clone(),
            objects,
            inputs,
            layout,
            state_buffer,
            addresses,
            arena: ArenaAllocator::new(allocator),
            reducer_types: post.reducer_types.clone(),
            scalar_types: post.scalar_types.clone(),
            scalar_executor: ExpressionExecutor::with_expressions_for_session(
                &post.scalar_expressions,
                query.session.as_ref(),
            ),
        }))
    }

    pub(crate) fn update(&mut self, payload: &Chunk) -> Result<()> {
        if payload.is_empty() {
            return Ok(());
        }
        let object = self
            .objects
            .first()
            .expect("input-rollup construction requires one aggregate");
        let input_indices = self
            .inputs
            .first()
            .expect("input-rollup construction requires one input mapping");
        let mut input_vectors = Vec::with_capacity(input_indices.len());
        for (argument_idx, (&column_idx, expected_type)) in input_indices
            .iter()
            .zip(object.function.arguments.iter())
            .enumerate()
        {
            let vector = payload.column(column_idx).ok_or_else(|| {
                paro_error::internal(format!(
                    "post-aggregate input-rollup argument {argument_idx} references missing payload column {column_idx}"
                ))
            })?;
            if vector.logical_type() != expected_type {
                return Err(paro_error::internal(format!(
                    "post-aggregate input-rollup argument type mismatch at {argument_idx}: expected={expected_type:?} actual={:?}",
                    vector.logical_type()
                )));
            }
            input_vectors.push(vector.as_ref());
        }
        let input_data = AggregateInputData::new(
            object.bind_info.as_deref(),
            &mut self.arena,
            AggregateCombineType::PreserveInput,
        );
        let update = object
            .function
            .simple_update
            .expect("input-rollup construction requires a simple update hook");
        let state = unsafe {
            self.state_buffer
                .as_mut_ptr()
                .cast::<u8>()
                .add(self.layout.state_offset(0))
        };
        // SAFETY: construction validated the bound argument types and state
        // layout; the state remains initialized and exclusively task-local.
        unsafe { update(&input_vectors, &input_data, state, payload.size()) };
        Ok(())
    }

    pub(crate) fn combine_from(&mut self, source: &mut Self) -> Result<()> {
        if self.source_indices != source.source_indices
            || self.layout.total_size() != source.layout.total_size()
            || self.objects.len() != source.objects.len()
            || !self
                .objects
                .iter()
                .zip(&source.objects)
                .all(|(left, right)| {
                    left.function.execution_semantics_equal(&right.function)
                        && function_data_equals(left.bind_info.as_ref(), right.bind_info.as_ref())
                })
        {
            return Err(paro_error::internal(
                "post-aggregate input-rollup states are incompatible",
            ));
        }
        let mut input_data =
            AggregateInputData::new(None, &mut self.arena, AggregateCombineType::PreserveInput);
        combine_states(
            &self.objects,
            &mut input_data,
            &source.addresses,
            &self.addresses,
            1,
        )
    }

    pub(crate) fn finish(mut self, query: &QueryRuntimeContext) -> Result<Box<[Arc<Vector>]>> {
        let allocator = query.allocator(MemoryTag::BaseTable);
        let mut reducer_values = Chunk::try_initialize(&self.reducer_types, 1, allocator.clone())?;
        let mut input_data =
            AggregateInputData::new(None, &mut self.arena, AggregateCombineType::PreserveInput);
        finalize_states(
            &self.objects,
            &mut input_data,
            &self.addresses,
            &mut reducer_values,
            1,
        )?;
        evaluate_post_reduction_scalars(
            &mut self.scalar_executor,
            &self.scalar_types,
            &reducer_values,
            query,
            allocator,
        )
    }
}

impl PostAggregateReducer {
    pub(crate) fn try_new(
        spec: &PostAggregateReductionSpec,
        query: &QueryRuntimeContext,
    ) -> Result<Self> {
        spec.verify()?;
        let objects = create_aggregate_objects(&spec.reducers)?;
        if objects.len() != spec.reducer_types.len() {
            return Err(paro_error::internal(format!(
                "post-aggregate reducer descriptor mismatch: reducers={} types={}",
                objects.len(),
                spec.reducer_types.len()
            )));
        }
        let mut inputs = Vec::with_capacity(spec.reducers.len());
        let mut input_types = Vec::with_capacity(spec.reducers.len());
        for (index, expression) in spec.reducers.iter().enumerate() {
            let Expression::Aggregate(aggregate) = expression else {
                return Err(paro_error::internal(format!(
                    "post-aggregate reducer {index} is not an aggregate expression"
                )));
            };
            if aggregate.is_distinct()
                || aggregate.filter.is_some()
                || !aggregate.order_bys.is_empty()
                || aggregate.function.destructor.is_some()
            {
                return Err(paro_error::internal(format!(
                    "post-aggregate reducer {index} must be an unfiltered, inline-state distributive aggregate"
                )));
            }
            let reducer_input_types = aggregate
                .children
                .iter()
                .map(Expression::return_type)
                .collect::<Vec<_>>();
            if reducer_input_types != aggregate.function.arguments {
                return Err(paro_error::internal(format!(
                    "post-aggregate reducer argument types disagree with its function at {index}: expressions={reducer_input_types:?} function={:?}",
                    aggregate.function.arguments
                )));
            }
            inputs.push(
                aggregate
                    .children
                    .iter()
                    .map(reference_index)
                    .collect::<Result<Vec<_>>>()?,
            );
            input_types.push(reducer_input_types);
            if aggregate.return_type != spec.reducer_types[index] {
                return Err(paro_error::internal(format!(
                    "post-aggregate reducer type mismatch at {index}: expression={:?} spec={:?}",
                    aggregate.return_type, spec.reducer_types[index]
                )));
            }
        }

        let layout = AggregateStateLayout::new(&objects)?;
        let allocator = query.allocator(MemoryTag::HashTable);
        let mut state_buffer = vec![0u64; state_buffer_words(layout.total_size())];
        initialize_state_buffer(&layout, &objects, &mut state_buffer, allocator.clone())?;
        Ok(Self {
            objects,
            inputs,
            input_types,
            aggregate_types: spec.aggregate_types.clone(),
            state_buffer,
            addresses: Vector::try_new(LogicalType::BigInt, VECTOR_SIZE, allocator.clone())?,
            arena: ArenaAllocator::new(allocator),
            reducer_types: spec.reducer_types.clone(),
            scalar_types: spec.scalar_types.clone(),
            scalar_executor: ExpressionExecutor::with_expressions_for_session(
                &spec.scalar_expressions,
                query.session.as_ref(),
            ),
        })
    }

    /// Feed one aggregate-only finalized batch into the reduction states.
    pub(crate) fn consume(&mut self, aggregates: &Chunk) -> Result<()> {
        if aggregates.is_empty() {
            return Ok(());
        }
        if aggregates.column_count() != self.aggregate_types.len() {
            return Err(paro_error::internal(format!(
                "post-aggregate input width mismatch: expected={} actual={}",
                self.aggregate_types.len(),
                aggregates.column_count()
            )));
        }
        for (index, expected_type) in self.aggregate_types.iter().enumerate() {
            let actual_type = aggregates
                .column(index)
                .expect("column count verified above")
                .logical_type();
            if actual_type != expected_type {
                return Err(paro_error::internal(format!(
                    "post-aggregate input type mismatch at {index}: expected={expected_type:?} actual={actual_type:?}"
                )));
            }
        }
        for (reducer_idx, (indices, types)) in self.inputs.iter().zip(&self.input_types).enumerate()
        {
            for (argument_idx, (&column_idx, expected_type)) in
                indices.iter().zip(types).enumerate()
            {
                let actual_type = aggregates
                    .column(column_idx)
                    .ok_or_else(|| {
                        paro_error::internal(format!(
                            "post-aggregate reducer {reducer_idx} input {argument_idx} is out of bounds"
                        ))
                    })?
                    .logical_type();
                if actual_type != expected_type {
                    return Err(paro_error::internal(format!(
                        "post-aggregate reducer input type mismatch: reducer={reducer_idx} argument={argument_idx} expected={expected_type:?} actual={actual_type:?}"
                    )));
                }
            }
        }
        fill_repeated_state_addresses(
            &mut self.addresses,
            self.state_buffer.as_mut_ptr().cast::<u8>(),
            aggregates.size(),
        )?;
        let payload = AggregatePayload {
            chunk: aggregates,
            aggregate_inputs: &self.inputs,
        };
        let mut input_data =
            AggregateInputData::new(None, &mut self.arena, AggregateCombineType::PreserveInput);
        update_states(
            &self.objects,
            &mut input_data,
            &payload,
            &self.addresses,
            aggregates.size(),
        )
    }

    /// Finalize the reducers, evaluate their bound scalar projection, and
    /// release reducer-owned arena storage when this object is dropped.
    pub(crate) fn finish(mut self, query: &QueryRuntimeContext) -> Result<Box<[Arc<Vector>]>> {
        self.finish_inner(query)
    }

    fn finish_inner(&mut self, query: &QueryRuntimeContext) -> Result<Box<[Arc<Vector>]>> {
        let allocator = query.allocator(MemoryTag::BaseTable);
        let addresses = single_state_addresses(
            self.state_buffer.as_mut_ptr().cast::<u8>(),
            allocator.clone(),
        )?;
        let mut reducer_values = Chunk::try_initialize(&self.reducer_types, 1, allocator.clone())?;
        let mut input_data =
            AggregateInputData::new(None, &mut self.arena, AggregateCombineType::PreserveInput);
        finalize_states(
            &self.objects,
            &mut input_data,
            &addresses,
            &mut reducer_values,
            1,
        )?;
        evaluate_post_reduction_scalars(
            &mut self.scalar_executor,
            &self.scalar_types,
            &reducer_values,
            query,
            allocator,
        )
    }
}

fn evaluate_post_reduction_scalars(
    executor: &mut ExpressionExecutor,
    scalar_types: &[LogicalType],
    reducer_values: &Chunk,
    query: &QueryRuntimeContext,
    allocator: Arc<dyn paro_common::allocator::Allocator>,
) -> Result<Box<[Arc<Vector>]>> {
    let mut scalar_values = Chunk::try_initialize(scalar_types, 1, allocator)?;
    executor.execute_all_kernel(
        VectorKernelInput::from_eval_input(ExpressionEvalInput {
            params: query.params.as_ref(),
            columns: reducer_values,
        })
        .with_count(1),
        query,
        &mut scalar_values,
    )?;
    if scalar_values.size() != 1 || scalar_values.column_count() != scalar_types.len() {
        return Err(paro_error::internal(format!(
            "post-aggregate scalar projection shape mismatch: rows={} columns={} expected_columns={}",
            scalar_values.size(),
            scalar_values.column_count(),
            scalar_types.len()
        )));
    }
    Ok(scalar_values.data.into_boxed_slice())
}

/// Per-emit-worker dynamic predicate compiled from ordinary HAVING plus the
/// post-reduction predicate. Scalar results are represented as constant
/// vectors and shared across every finalized group batch.
#[derive(Debug)]
pub(crate) struct PostAggregateFilterLocal {
    executor: ExpressionExecutor,
    aggregate_types: Box<[LogicalType]>,
    scalar_types: Box<[LogicalType]>,
    scalar_values: Box<[Arc<Vector>]>,
    scalar_vectors: Option<Box<[Arc<Vector>]>>,
}

impl PostAggregateFilterLocal {
    pub(crate) fn new(
        post: &PostAggregateReductionSpec,
        having: &[Expression],
        scalar_values: &[Arc<Vector>],
        query: &QueryRuntimeContext,
    ) -> Result<Self> {
        post.verify()?;
        if scalar_values.len() != post.scalar_types.len() {
            return Err(paro_error::internal(format!(
                "post-aggregate scalar result mismatch: values={} types={}",
                scalar_values.len(),
                post.scalar_types.len()
            )));
        }
        for (index, (value, expected_type)) in scalar_values
            .iter()
            .zip(post.scalar_types.iter())
            .enumerate()
        {
            if value.len() != 1 || value.logical_type() != expected_type {
                return Err(paro_error::internal(format!(
                    "post-aggregate scalar value shape mismatch at {index}: expected_type={expected_type:?} actual_type={:?} rows={}",
                    value.logical_type(),
                    value.len()
                )));
            }
        }
        let mut predicates = having.to_vec();
        predicates.push(post.predicate.clone());
        let predicate = if predicates.len() == 1 {
            predicates.pop().expect("one post-aggregate predicate")
        } else {
            Expression::Conjunction(ConjunctionExpression {
                conjunction_type: ConjunctionType::And,
                children: predicates,
            })
        };
        Ok(Self {
            executor: ExpressionExecutor::with_expressions_for_session(
                std::slice::from_ref(&predicate),
                query.session.as_ref(),
            ),
            aggregate_types: post.aggregate_types.clone(),
            scalar_types: post.scalar_types.clone(),
            scalar_values: scalar_values.to_vec().into_boxed_slice(),
            scalar_vectors: None,
        })
    }

    pub(crate) fn select(
        &mut self,
        aggregates: &Chunk,
        count: usize,
        query: &QueryRuntimeContext,
        selection: &mut SelectionVector,
    ) -> Result<usize> {
        if aggregates.column_count() != self.aggregate_types.len() {
            return Err(paro_error::internal(format!(
                "post-aggregate predicate input width mismatch: expected={} actual={}",
                self.aggregate_types.len(),
                aggregates.column_count()
            )));
        }
        for (index, expected_type) in self.aggregate_types.iter().enumerate() {
            let actual_type = aggregates
                .column(index)
                .expect("column count verified above")
                .logical_type();
            if actual_type != expected_type {
                return Err(paro_error::internal(format!(
                    "post-aggregate predicate input type mismatch at {index}: expected={expected_type:?} actual={actual_type:?}"
                )));
            }
        }
        if self.scalar_vectors.is_none()
            || self
                .scalar_vectors
                .as_ref()
                .is_some_and(|vectors| vectors.first().is_some_and(|vector| vector.len() != count))
        {
            let allocator = aggregates.allocator().clone();
            self.scalar_vectors = Some(
                self.scalar_types
                    .iter()
                    .cloned()
                    .zip(self.scalar_values.iter())
                    .map(|(ty, value)| {
                        Ok(Arc::new(Vector::try_constant_from_value(
                            ty,
                            value.get_value(0),
                            count,
                            allocator.clone(),
                        )?))
                    })
                    .collect::<Result<Vec<_>>>()?
                    .into_boxed_slice(),
            );
        }

        let scalar_vectors = self
            .scalar_vectors
            .as_ref()
            .expect("post-aggregate scalar vectors initialized above");
        let mut columns = Vec::with_capacity(aggregates.column_count() + scalar_vectors.len());
        columns.extend(aggregates.data.iter().cloned());
        columns.extend(scalar_vectors.iter().cloned());
        let mut input = Chunk::from_arc_vectors(columns, aggregates.allocator().clone());
        input.try_set_cardinality(count)?;
        self.executor.select_kernel(
            0,
            VectorKernelInput::from_eval_input(ExpressionEvalInput {
                params: query.params.as_ref(),
                columns: &input,
            })
            .with_count(count),
            query,
            selection,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use paro_common::runtime_value::Value;
    use paro_context::test_support::TestStatementContextBuilder;
    use paro_function::aggregate::distributive::sum::get_sum_function;
    use paro_function::scalar::math::get_abs_functions;
    use paro_planner::expression::{
        AggregateExpression, ComparisonExpression, ComparisonType, ConstantExpression,
        FunctionExpression, OperatorExpression, OperatorType, ReferenceExpression,
    };

    use crate::memory_runtime::QueryMemoryPool;
    use crate::physical::specs::{AggregateSpec, GroupKeyEncoding};
    use crate::runtime::{ParameterBindings, QueryOutputPort, QueryRuntimeContext};

    fn query_context() -> QueryRuntimeContext {
        QueryRuntimeContext::new(
            TestStatementContextBuilder::minimal().build(),
            Arc::new(ParameterBindings::empty()),
            Arc::new(QueryMemoryPool::unbounded()),
            QueryOutputPort::unbounded(),
        )
    }

    fn decimal_sum_reduction_spec() -> PostAggregateReductionSpec {
        let decimal = LogicalType::Decimal {
            precision: 38,
            scale: 0,
        };
        let (sum, _) = get_sum_function()
            .bind(std::slice::from_ref(&decimal))
            .expect("bind decimal sum");
        let merge = sum
            .partial_merge_function()
            .expect("decimal sum has a closed partial merge");
        PostAggregateReductionSpec {
            aggregate_types: vec![decimal.clone()].into_boxed_slice(),
            reducers: vec![Expression::Aggregate(AggregateExpression::new(
                merge,
                vec![Expression::Reference(ReferenceExpression::new(
                    0,
                    decimal.clone(),
                ))],
                decimal.clone(),
            ))]
            .into_boxed_slice(),
            reducer_types: vec![decimal.clone()].into_boxed_slice(),
            scalar_expressions: vec![Expression::Reference(ReferenceExpression::new(
                0,
                decimal.clone(),
            ))]
            .into_boxed_slice(),
            scalar_types: vec![decimal.clone()].into_boxed_slice(),
            predicate: Expression::Comparison(ComparisonExpression::new(
                ComparisonType::GreaterThan,
                Expression::Reference(ReferenceExpression::new(0, decimal.clone())),
                Expression::Reference(ReferenceExpression::new(1, decimal)),
            )),
            input_rollup_sources: None,
        }
    }

    fn aggregate_spec_with_post(
        post: PostAggregateReductionSpec,
        having_filter: Box<[Expression]>,
    ) -> AggregateSpec {
        let decimal = post.aggregate_types[0].clone();
        let mut aggregate = post.reducers[0].clone();
        let Expression::Aggregate(bound) = &mut aggregate else {
            panic!("test post reducer must be an aggregate");
        };
        bound.children = vec![Expression::Reference(ReferenceExpression::new(
            1,
            decimal.clone(),
        ))];
        AggregateSpec {
            grouping_key_count: 1,
            state_output_projection: Box::new([]),
            estimated_input_rows: None,
            projection_exprs: Box::new([]),
            payload_types: Box::new([LogicalType::Integer, decimal.clone()]),
            groups: Box::new([Expression::Reference(ReferenceExpression::new(
                0,
                LogicalType::Integer,
            ))]),
            group_key_encodings: Box::new([GroupKeyEncoding::Identity]),
            grouping_sets: Box::new([]),
            aggregates: Box::new([aggregate]),
            grouping_functions: Box::new([]),
            aggregate_inputs: Box::new([Box::new([1])]),
            aggregate_filters: Box::new([None]),
            aggregate_orders: Box::new([Box::new([])]),
            post_reduction: Some(post),
            having_filter,
            perfect_hash: None,
            output_names: Box::new(["key".to_string(), "value".to_string()]),
            output_types: Box::new([LogicalType::Integer, decimal]),
        }
    }

    fn reducer_init_error(
        spec: &PostAggregateReductionSpec,
        query: &QueryRuntimeContext,
    ) -> String {
        match PostAggregateReducer::try_new(spec, query) {
            Ok(_) => panic!("malformed post-reduction spec reached reducer initialization"),
            Err(error) => error.to_string(),
        }
    }

    #[test]
    fn decimal_reduction_is_exact_across_i128_intermediate_overflow() {
        let query = query_context();
        let spec = decimal_sum_reduction_spec();
        let decimal = spec.reducer_types[0].clone();
        let maximum = 10_i128.pow(38) - 1;
        let mut reducer = PostAggregateReducer::try_new(&spec, &query).expect("reducer");

        let mut first = Chunk::try_initialize(
            std::slice::from_ref(&decimal),
            2,
            query.allocator(MemoryTag::BaseTable),
        )
        .expect("first batch");
        first.set_cardinality(2);
        first.column_mut(0).unwrap().set_i128(0, maximum);
        first.column_mut(0).unwrap().set_i128(1, maximum);
        reducer.consume(&first).expect("consume first batch");

        let mut second = Chunk::try_initialize(
            std::slice::from_ref(&decimal),
            2,
            query.allocator(MemoryTag::BaseTable),
        )
        .expect("second batch");
        second.set_cardinality(2);
        second.column_mut(0).unwrap().set_i128(0, -maximum);
        second.column_mut(0).unwrap().set_i128(1, 0);
        second.column_mut(0).unwrap().set_null(1, true);
        reducer.consume(&second).expect("consume second batch");

        let result = reducer.finish(&query).expect("finalize");
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].get_value(0), Value::Decimal(maximum, 38, 0));
    }

    #[test]
    fn dynamic_filter_uses_typed_scalar_and_sql_null_semantics() {
        let query = query_context();
        let spec = decimal_sum_reduction_spec();
        let decimal = spec.scalar_types[0].clone();
        let mut groups = Chunk::try_initialize(
            std::slice::from_ref(&decimal),
            3,
            query.allocator(MemoryTag::BaseTable),
        )
        .expect("group values");
        groups.set_cardinality(3);
        groups.column_mut(0).unwrap().set_i128(0, 5);
        groups.column_mut(0).unwrap().set_i128(1, 10);
        groups.column_mut(0).unwrap().set_null(2, true);
        let mut selection =
            SelectionVector::try_with_capacity(3, query.allocator(MemoryTag::BaseTable))
                .expect("selection");
        let scalar = Arc::new(
            Vector::try_constant_from_value(
                decimal.clone(),
                Value::Decimal(7, 38, 0),
                1,
                query.allocator(MemoryTag::BaseTable),
            )
            .expect("scalar"),
        );
        let mut filter =
            PostAggregateFilterLocal::new(&spec, &[], &[scalar], &query).expect("filter");

        let selected = filter
            .select(&groups, 3, &query, &mut selection)
            .expect("filter");
        assert_eq!(selected, 1);
        assert_eq!(selection.get(0), 1);

        let null_scalar = Arc::new(
            Vector::try_constant_from_value(
                decimal.clone(),
                Value::Null(decimal),
                1,
                query.allocator(MemoryTag::BaseTable),
            )
            .expect("NULL scalar"),
        );
        let mut null_filter =
            PostAggregateFilterLocal::new(&spec, &[], &[null_scalar], &query).expect("NULL filter");
        let selected = null_filter
            .select(&groups, 3, &query, &mut selection)
            .expect("NULL filter");
        assert_eq!(selected, 0);
    }

    #[test]
    fn reducer_rejects_mismatched_unsafe_input_vector_type() {
        let query = query_context();
        let spec = decimal_sum_reduction_spec();
        let mut reducer = PostAggregateReducer::try_new(&spec, &query).expect("reducer");
        let mut wrong = Chunk::try_initialize(
            &[LogicalType::BigInt],
            1,
            query.allocator(MemoryTag::BaseTable),
        )
        .expect("wrong input");
        wrong.set_cardinality(1);
        wrong.column_mut(0).unwrap().set_i64(0, 1);
        assert!(reducer
            .consume(&wrong)
            .expect_err("type mismatch")
            .to_string()
            .contains("input type mismatch"));
    }

    #[test]
    fn runtime_boundary_rejects_reducer_function_return_mismatch() {
        let query = query_context();
        let mut spec = decimal_sum_reduction_spec();
        let Expression::Aggregate(reducer) = &mut spec.reducers[0] else {
            panic!("expected reducer aggregate");
        };
        reducer.return_type = LogicalType::BigInt;
        spec.reducer_types[0] = LogicalType::BigInt;

        let error = reducer_init_error(&spec, &query);
        assert!(error.contains("reducer type mismatch"), "{error}");
    }

    #[test]
    fn runtime_boundary_rejects_scalar_function_argument_mismatch() {
        let query = query_context();
        let mut spec = decimal_sum_reduction_spec();
        let decimal = spec.reducer_types[0].clone();
        let (abs, _) = get_abs_functions()
            .bind(&[LogicalType::BigInt])
            .expect("bind abs(bigint)");
        spec.scalar_expressions = Box::new([Expression::Function(FunctionExpression::new(
            abs,
            vec![Expression::Reference(ReferenceExpression::new(0, decimal))],
            LogicalType::BigInt,
        ))]);
        spec.scalar_types = Box::new([LogicalType::BigInt]);
        spec.predicate = Expression::Operator(OperatorExpression::new_unary(
            OperatorType::IsNotNull,
            Expression::Reference(ReferenceExpression::new(1, LogicalType::BigInt)),
            LogicalType::Boolean,
        ));

        let error = reducer_init_error(&spec, &query);
        assert!(
            error.contains("scalar function abs argument 0 type mismatch"),
            "{error}"
        );
    }

    #[test]
    fn runtime_boundary_rejects_mixed_physical_comparison_operands() {
        let query = query_context();
        let mut spec = decimal_sum_reduction_spec();
        let hidden_type = spec.scalar_types[0].clone();
        spec.predicate = Expression::Comparison(ComparisonExpression::new(
            ComparisonType::Equal,
            Expression::Reference(ReferenceExpression::new(1, hidden_type)),
            Expression::Constant(ConstantExpression::new(
                Value::BigInt(1),
                LogicalType::BigInt,
            )),
        ));

        let error = reducer_init_error(&spec, &query);
        assert!(
            error.contains("comparison operand type mismatch"),
            "{error}"
        );
    }

    #[test]
    fn aggregate_runtime_boundary_rejects_having_reference_outside_its_domain() {
        let post = decimal_sum_reduction_spec();
        let having = Expression::Comparison(ComparisonExpression::new(
            ComparisonType::Equal,
            Expression::Reference(ReferenceExpression::new(0, LogicalType::BigInt)),
            Expression::Constant(ConstantExpression::new(
                Value::BigInt(1),
                LogicalType::BigInt,
            )),
        ));
        let spec = aggregate_spec_with_post(post, Box::new([having]));

        let error = spec
            .verify_post_reduction()
            .expect_err("HAVING must be checked against finalized aggregate types");
        assert!(
            error
                .to_string()
                .contains("HAVING expression 0 reference 0 type mismatch"),
            "{error}"
        );
    }

    #[test]
    fn metadata_rich_varchar_scalar_survives_publication_and_filtering() {
        let query = query_context();
        let mut spec = decimal_sum_reduction_spec();
        let collation = LogicalType::VarcharCollation("C".to_string());
        spec.scalar_expressions = Box::new([Expression::Constant(ConstantExpression::new(
            Value::Varchar("anchor".to_string()),
            collation.clone(),
        ))]);
        spec.scalar_types = Box::new([collation.clone()]);
        spec.predicate = Expression::Operator(OperatorExpression::new_unary(
            OperatorType::IsNotNull,
            Expression::Reference(ReferenceExpression::new(1, collation.clone())),
            LogicalType::Boolean,
        ));

        let scalars = PostAggregateReducer::try_new(&spec, &query)
            .expect("typed reducer")
            .finish(&query)
            .expect("publish typed scalar vectors");
        assert_eq!(scalars.len(), 1);
        assert_eq!(scalars[0].logical_type(), &collation);
        assert_eq!(scalars[0].get_string(0), Some("anchor"));

        let aggregate_type = spec.aggregate_types[0].clone();
        let mut aggregates = Chunk::try_initialize(
            std::slice::from_ref(&aggregate_type),
            2,
            query.allocator(MemoryTag::BaseTable),
        )
        .expect("aggregate values");
        aggregates.set_cardinality(2);
        aggregates.column_mut(0).unwrap().set_i128(0, 1);
        aggregates.column_mut(0).unwrap().set_i128(1, 2);
        let mut selection =
            SelectionVector::try_with_capacity(2, query.allocator(MemoryTag::BaseTable))
                .expect("selection");
        let mut filter = PostAggregateFilterLocal::new(&spec, &[], &scalars, &query)
            .expect("typed scalar filter");

        assert_eq!(
            filter
                .select(&aggregates, 2, &query, &mut selection)
                .expect("filter"),
            2
        );
        assert_eq!(selection.get(0), 0);
        assert_eq!(selection.get(1), 1);
    }
}
