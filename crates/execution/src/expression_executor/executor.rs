// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Compiled expression executor.

use std::collections::HashSet;
use std::sync::Arc;

use paro_common::allocator::{Allocator, MemoryTag};
use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use paro_common::vector::{DictionarySource, SelectionVector, Vector, VectorType};
use paro_function::scalar::cast::CastExecCtx;
use paro_function::scalar::executor::variadic::apply_coalesce_child;
use paro_function::scalar::operators::logic::LogicExecutor;
use paro_function::scalar::{
    DictionaryStrategy, FunctionErrorMode, FunctionExecContext, FunctionSideEffects,
    FunctionStability,
};
use paro_planner::expression::{
    CaseExpression, CastExpression, ColumnRefExpression, ComparisonExpression,
    ConjunctionExpression, ConstantExpression, Expression, FunctionExpression, OperatorExpression,
    OperatorType, ReferenceExpression,
};

use super::comparison::{compile_comparison_dispatch, COMPARISON_EXEC_CTX};
use super::execution_state::BoundFunctionContext;
use super::predicate::{
    accumulate_selected_rows, build_marked_selection, copy_selection, scan_bool_selection,
    scan_false_bool_selection, scan_null_selection, select_all_rows,
};
use super::state::{
    CachedDictionaryInputId, CaseExpressionState, CastExpressionState, ColumnRefExpressionState,
    ComparisonExpressionState, CompiledExpressionState, ConjunctionExpressionState,
    ConstantExpressionState, EvaluatedValue, ExecuteFunctionState, OperatorExpressionState,
    PreparedInList, ReferenceExpressionState, ValueSlot, WindowExpressionState,
};

#[derive(Debug)]
pub struct CompiledExpressionProgram {
    expressions: Vec<Expression>,
}

#[derive(Debug)]
pub struct CompiledExecutorState {
    states: Vec<CompiledExpressionState>,
}

#[derive(Debug)]
pub struct ExpressionExecutor {
    pub program: CompiledExpressionProgram,
    pub state: CompiledExecutorState,
}

impl ExpressionExecutor {
    fn assert_no_subquery_expression(expr: &Expression) {
        match expr {
            Expression::Function(func) => {
                for child in &func.children {
                    Self::assert_no_subquery_expression(child);
                }
            }
            Expression::Cast(cast) => Self::assert_no_subquery_expression(&cast.child),
            Expression::Comparison(comp) => {
                Self::assert_no_subquery_expression(&comp.left);
                Self::assert_no_subquery_expression(&comp.right);
            }
            Expression::Conjunction(conj) => {
                for child in &conj.children {
                    Self::assert_no_subquery_expression(child);
                }
            }
            Expression::Case(case) => {
                Self::assert_no_subquery_expression(&case.check);
                Self::assert_no_subquery_expression(&case.result_if_true);
                Self::assert_no_subquery_expression(&case.result_if_false);
            }
            Expression::Operator(op) => {
                for child in &op.children {
                    Self::assert_no_subquery_expression(child);
                }
            }
            Expression::Aggregate(agg) => {
                for child in &agg.children {
                    Self::assert_no_subquery_expression(child);
                }
                if let Some(filter) = &agg.filter {
                    Self::assert_no_subquery_expression(filter);
                }
                for order in &agg.order_bys {
                    Self::assert_no_subquery_expression(&order.expression);
                }
            }
            Expression::Window(window) => {
                for child in &window.children {
                    Self::assert_no_subquery_expression(child);
                }
                for partition in &window.partitions {
                    Self::assert_no_subquery_expression(partition);
                }
                for order in &window.orders {
                    Self::assert_no_subquery_expression(&order.expression);
                }
            }
            Expression::Subquery(_) => {
                panic!(
                    "ExpressionExecutor invariant violated: Expression::Subquery must be flattened before execution"
                );
            }
            Expression::Constant(_) | Expression::ColumnRef(_) | Expression::Reference(_) => {}
        }
    }

    pub fn new(expr: &Expression) -> Self {
        Self::with_expressions(std::slice::from_ref(expr))
    }

    pub fn with_expressions(exprs: &[Expression]) -> Self {
        let expressions = exprs.to_vec();
        let states = expressions.iter().map(Self::initialize).collect();
        Self {
            program: CompiledExpressionProgram { expressions },
            state: CompiledExecutorState { states },
        }
    }

    pub fn expression_count(&self) -> usize {
        self.program.expressions.len()
    }

    #[cfg(test)]
    pub(crate) fn compiled_state(&self, expr_idx: usize) -> &CompiledExpressionState {
        &self.state.states[expr_idx]
    }

    pub fn initialize(expr: &Expression) -> CompiledExpressionState {
        Self::assert_no_subquery_expression(expr);
        match expr {
            Expression::Function(e) => CompiledExpressionState::Function(ExecuteFunctionState {
                child_states: e.children.iter().map(Self::initialize).collect(),
                intermediate_types: e.children.iter().map(Expression::return_type).collect(),
                intermediate_chunk: None,
                local_state: None,
                cached_dictionary_input_id: None,
                cached_dictionary_output: None,
                result: ValueSlot::default(),
            }),
            Expression::Cast(e) => CompiledExpressionState::Cast(CastExpressionState {
                child: Box::new(Self::initialize(&e.child)),
                child_result: ValueSlot::default(),
                result: ValueSlot::default(),
            }),
            Expression::Comparison(e) => {
                let dispatch =
                    compile_comparison_dispatch(&e.left.return_type(), e.comparison_type);
                CompiledExpressionState::Comparison(ComparisonExpressionState {
                    left: Box::new(Self::initialize(&e.left)),
                    right: Box::new(Self::initialize(&e.right)),
                    compare: dispatch.compare,
                    select: dispatch.select,
                    left_result: ValueSlot::default(),
                    right_result: ValueSlot::default(),
                    result: ValueSlot::default(),
                })
            }
            Expression::Conjunction(e) => {
                CompiledExpressionState::Conjunction(ConjunctionExpressionState {
                    child_states: e.children.iter().map(Self::initialize).collect(),
                    ping: ValueSlot::default(),
                    pong: ValueSlot::default(),
                })
            }
            Expression::Case(e) => CompiledExpressionState::Case(CaseExpressionState {
                check: Box::new(Self::initialize(&e.check)),
                result_if_true: Box::new(Self::initialize(&e.result_if_true)),
                result_if_false: Box::new(Self::initialize(&e.result_if_false)),
                check_result: ValueSlot::default(),
                true_result: ValueSlot::default(),
                false_result: ValueSlot::default(),
                result: ValueSlot::default(),
            }),
            Expression::Operator(e) => CompiledExpressionState::Operator(OperatorExpressionState {
                child_states: e.children.iter().map(Self::initialize).collect(),
                child_results: (0..e.children.len())
                    .map(|_| ValueSlot::default())
                    .collect(),
                in_list: Self::prepare_in_list(e),
                result: ValueSlot::default(),
                aux: ValueSlot::default(),
                scratch: ValueSlot::default(),
            }),
            Expression::Constant(_) => CompiledExpressionState::Constant(ConstantExpressionState),
            Expression::ColumnRef(_) => {
                CompiledExpressionState::ColumnRef(ColumnRefExpressionState)
            }
            Expression::Reference(_) => {
                CompiledExpressionState::Reference(ReferenceExpressionState)
            }
            Expression::Aggregate(_) => {
                panic!("Aggregate expressions should not be initialized by ExpressionExecutor");
            }
            Expression::Subquery(_) => {
                panic!(
                    "ExpressionExecutor invariant violated: Expression::Subquery must be flattened before execution"
                );
            }
            Expression::Window(_) => CompiledExpressionState::Window(WindowExpressionState),
        }
    }

    pub fn execute_into(
        &mut self,
        expr_idx: usize,
        input: &Chunk,
        sel: Option<&SelectionVector>,
        count: usize,
        runtime: &dyn FunctionExecContext,
        result: &mut Vector,
    ) -> Result<()> {
        let expr = &self.program.expressions[expr_idx];
        let state = &mut self.state.states[expr_idx];
        Self::execute_into_inner(expr, state, input, sel, count, runtime, result)
    }

    pub fn execute_all_into(
        &mut self,
        input: &Chunk,
        runtime: &dyn FunctionExecContext,
        result: &mut Chunk,
    ) -> Result<()> {
        let output_types: Vec<LogicalType> = self
            .program
            .expressions
            .iter()
            .map(Expression::return_type)
            .collect();
        Self::prepare_output_chunk(
            result,
            &output_types,
            input.size(),
            runtime.allocator(MemoryTag::BaseTable),
        )?;
        for expr_idx in 0..self.program.expressions.len() {
            let column = result.column_mut(expr_idx).ok_or_else(|| {
                paro_error::internal(format!("Output column {} not found", expr_idx))
            })?;
            self.execute_into(expr_idx, input, None, input.size(), runtime, column)?;
        }
        result.set_cardinality(input.size());
        Ok(())
    }

    pub fn select_into(
        &mut self,
        expr_idx: usize,
        input: &Chunk,
        count: usize,
        runtime: &dyn FunctionExecContext,
        sel: &mut SelectionVector,
    ) -> Result<usize> {
        let expr = &self.program.expressions[expr_idx];
        let state = &mut self.state.states[expr_idx];
        Self::select_expression(expr, state, input, None, count, runtime, sel)
    }

    pub fn execute_expression(
        &mut self,
        expr_idx: usize,
        chunk: &Chunk,
        sel: Option<&SelectionVector>,
        count: usize,
        runtime: &dyn FunctionExecContext,
    ) -> Result<Arc<Vector>> {
        let mut result = Vector::try_new(
            self.program.expressions[expr_idx].return_type(),
            count.max(1),
            runtime.allocator(MemoryTag::BaseTable),
        )?;
        self.execute_into(expr_idx, chunk, sel, count, runtime, &mut result)?;
        Ok(Arc::new(result))
    }

    fn execute_into_inner(
        expr: &Expression,
        state: &mut CompiledExpressionState,
        chunk: &Chunk,
        sel: Option<&SelectionVector>,
        count: usize,
        runtime: &dyn FunctionExecContext,
        result: &mut Vector,
    ) -> Result<()> {
        match (expr, state) {
            (Expression::Function(expr), CompiledExpressionState::Function(state)) => {
                Self::execute_function_into(expr, state, chunk, sel, count, runtime, result)
            }
            (Expression::Cast(expr), CompiledExpressionState::Cast(state)) => {
                Self::execute_cast_into(expr, state, chunk, sel, count, runtime, result)
            }
            (Expression::Comparison(expr), CompiledExpressionState::Comparison(state)) => {
                Self::execute_comparison_into(expr, state, chunk, sel, count, runtime, result)
            }
            (Expression::Conjunction(expr), CompiledExpressionState::Conjunction(state)) => {
                Self::execute_conjunction_into(expr, state, chunk, sel, count, runtime, result)
            }
            (Expression::Case(expr), CompiledExpressionState::Case(state)) => {
                Self::execute_case_into(expr, state, chunk, sel, count, runtime, result)
            }
            (Expression::Operator(expr), CompiledExpressionState::Operator(state)) => {
                Self::execute_operator_into(expr, state, chunk, sel, count, runtime, result)
            }
            (Expression::Constant(expr), CompiledExpressionState::Constant(_)) => {
                Self::execute_constant_into(expr, count, runtime, result)
            }
            (Expression::ColumnRef(expr), CompiledExpressionState::ColumnRef(_)) => {
                Self::execute_column_ref_into(expr, chunk, sel, count, result)
            }
            (Expression::Reference(expr), CompiledExpressionState::Reference(_)) => {
                Self::execute_reference_into(expr, chunk, sel, count, result)
            }
            (Expression::Aggregate(_), _) => Err(paro_error::internal(
                "Aggregate expressions should not be executed by ExpressionExecutor",
            )),
            (Expression::Subquery(_), _) => Err(paro_error::not_implemented(
                "Subquery execution in ExpressionExecutor",
            )),
            (Expression::Window(_), _) => Err(paro_error::not_implemented(
                "Window execution in ExpressionExecutor",
            )),
            _ => Err(paro_error::internal("Expression state mismatch")),
        }
    }

    fn execute_value(
        expr: &Expression,
        state: &mut CompiledExpressionState,
        chunk: &Chunk,
        sel: Option<&SelectionVector>,
        count: usize,
        runtime: &dyn FunctionExecContext,
    ) -> Result<EvaluatedValue> {
        match (expr, state) {
            (Expression::Function(expr), CompiledExpressionState::Function(state)) => {
                Self::execute_function_value(expr, state, chunk, sel, count, runtime)
            }
            (Expression::Cast(expr), CompiledExpressionState::Cast(state)) => {
                Self::execute_cast_value(expr, state, chunk, sel, count, runtime)
            }
            (Expression::Comparison(expr), CompiledExpressionState::Comparison(state)) => {
                Self::execute_comparison_value(expr, state, chunk, sel, count, runtime)
            }
            (Expression::Conjunction(expr), CompiledExpressionState::Conjunction(state)) => {
                Self::execute_conjunction_value(expr, state, chunk, sel, count, runtime)
            }
            (Expression::Case(expr), CompiledExpressionState::Case(state)) => {
                Self::execute_case_value(expr, state, chunk, sel, count, runtime)
            }
            (Expression::Operator(expr), CompiledExpressionState::Operator(state)) => {
                Self::execute_operator_value(expr, state, chunk, sel, count, runtime)
            }
            (Expression::Constant(expr), CompiledExpressionState::Constant(_)) => {
                Ok(EvaluatedValue::Borrowed(Vector::try_constant_from_value(
                    expr.return_type.clone(),
                    expr.value.clone(),
                    count,
                    runtime.allocator(MemoryTag::BaseTable),
                )?))
            }
            (Expression::ColumnRef(expr), CompiledExpressionState::ColumnRef(_)) => {
                Self::execute_column_ref_value(expr, chunk, sel, count)
            }
            (Expression::Reference(expr), CompiledExpressionState::Reference(_)) => {
                Self::execute_reference_value(expr, chunk, sel, count)
            }
            (Expression::Aggregate(_), _) => Err(paro_error::internal(
                "Aggregate expressions should not be executed by ExpressionExecutor",
            )),
            (Expression::Subquery(_), _) => Err(paro_error::not_implemented(
                "Subquery execution in ExpressionExecutor",
            )),
            (Expression::Window(_), _) => Err(paro_error::not_implemented(
                "Window execution in ExpressionExecutor",
            )),
            _ => Err(paro_error::internal("Expression state mismatch")),
        }
    }

    fn prepare_output_chunk(
        result: &mut Chunk,
        types: &[LogicalType],
        count: usize,
        allocator: Arc<dyn Allocator>,
    ) -> Result<()> {
        let required_capacity = count.max(1);
        let needs_reinit = result.column_count() != types.len()
            || result.capacity() < required_capacity
            || result.types() != types;
        if needs_reinit {
            *result = Chunk::try_initialize(types, required_capacity, allocator)?;
        } else {
            result.try_reset(result.allocator().clone())?;
        }
        Ok(())
    }

    fn prepare_result_vector(
        result: &mut Vector,
        logical_type: &LogicalType,
        count: usize,
        allocator: Arc<dyn Allocator>,
    ) -> Result<()> {
        let required_capacity = count.max(1);
        if result.logical_type() != logical_type || result.capacity() < required_capacity {
            *result = Vector::try_new(logical_type.clone(), required_capacity, allocator)?;
        } else {
            result.try_reset_for_execution(required_capacity, allocator)?;
        }
        result.set_len(count);
        Ok(())
    }

    fn prepare_slot_result<'a>(
        slot: &'a mut ValueSlot,
        logical_type: &LogicalType,
        count: usize,
        allocator: Arc<dyn Allocator>,
    ) -> Result<&'a mut Vector> {
        slot.prepare_scratch(logical_type, count, allocator)
    }

    fn prepare_intermediate_chunk<'a>(
        intermediate_types: &[LogicalType],
        intermediate_chunk: &'a mut Option<Chunk>,
        count: usize,
        allocator: Arc<dyn Allocator>,
    ) -> Result<&'a mut Chunk> {
        let required_capacity = count.max(1);
        let needs_reinit = intermediate_chunk
            .as_ref()
            .map(|chunk| {
                chunk.capacity() < required_capacity || chunk.types() != intermediate_types
            })
            .unwrap_or(true);
        if needs_reinit {
            *intermediate_chunk = Some(Chunk::try_initialize(
                intermediate_types,
                required_capacity,
                allocator,
            )?);
        } else if let Some(chunk) = intermediate_chunk.as_mut() {
            chunk.try_reset(chunk.allocator().clone())?;
        }
        let chunk = intermediate_chunk
            .as_mut()
            .expect("intermediate chunk initialized");
        chunk.set_cardinality(count);
        Ok(chunk)
    }

    fn store_value(slot: &mut ValueSlot, value: &EvaluatedValue) {
        slot.set_value(value.as_vector().reference());
    }

    fn ensure_function_local_state(
        function: &paro_function::scalar::BoundScalarFunction,
        local_state: &mut Option<Box<dyn paro_function::scalar::FunctionLocalState>>,
        runtime: &dyn FunctionExecContext,
    ) -> Result<()> {
        if local_state.is_none() {
            if let Some(init_local_state) = function.init_local_state {
                *local_state = Some(init_local_state(runtime, function.bind_data.as_deref())?);
            }
        }
        Ok(())
    }

    fn build_dictionary_cache_input(
        intermediate: &Chunk,
        input_idx: usize,
        dictionary_child: Arc<Vector>,
        unique_len: usize,
    ) -> Result<Chunk> {
        let mut inputs = Vec::with_capacity(intermediate.column_count());
        for (child_idx, column) in intermediate.data.iter().enumerate() {
            if child_idx == input_idx {
                inputs.push(dictionary_child.clone());
                continue;
            }

            let mut argument = column.as_ref().reference();
            argument.set_count(unique_len);
            inputs.push(Arc::new(argument));
        }
        Ok(Chunk::from_arc_vectors(
            inputs,
            intermediate.allocator().clone(),
        ))
    }

    fn try_dictionary_cached_function(
        expr: &FunctionExpression,
        intermediate: &Chunk,
        count: usize,
        runtime: &dyn FunctionExecContext,
        allocator: Arc<dyn Allocator>,
        local_state: Option<&dyn paro_function::scalar::FunctionLocalState>,
        cached_dictionary_input_id: &mut Option<CachedDictionaryInputId>,
        cached_dictionary_output: &mut Option<Arc<Vector>>,
    ) -> Result<Option<Vector>> {
        let DictionaryStrategy::StorageDictionaryCache { input_idx } =
            expr.function.dictionary_strategy
        else {
            return Ok(None);
        };

        if expr.function.stability != FunctionStability::Consistent
            || expr.function.side_effects != FunctionSideEffects::NoSideEffects
            || expr.function.error_mode != FunctionErrorMode::Infallible
        {
            return Ok(None);
        }

        let driving_input = intermediate
            .column(input_idx)
            .ok_or_else(|| paro_error::internal("dictionary cache driving input missing"))?;
        let Some(dictionary_info) = driving_input.dictionary_info() else {
            return Ok(None);
        };
        if dictionary_info.source != DictionarySource::Storage {
            return Ok(None);
        }
        let Some(provenance_id) = dictionary_info.provenance_id else {
            return Ok(None);
        };

        let driving_selection = driving_input
            .sel_vector()
            .ok_or_else(|| paro_error::internal("dictionary cache driving selection missing"))?;
        let dictionary_child = driving_input
            .child()
            .cloned()
            .ok_or_else(|| paro_error::internal("dictionary cache driving child missing"))?;
        if dictionary_child.len() != dictionary_info.unique_len {
            return Ok(None);
        }

        for (child_idx, column) in intermediate.data.iter().enumerate() {
            if child_idx == input_idx {
                continue;
            }
            if column.vector_type() != VectorType::Constant {
                return Ok(None);
            }
        }

        let cache_id = CachedDictionaryInputId {
            provenance_id,
            unique_len: dictionary_info.unique_len,
        };

        let cached_output = if *cached_dictionary_input_id == Some(cache_id) {
            cached_dictionary_output.as_ref().cloned()
        } else {
            None
        };

        let output_child = if let Some(output) = cached_output {
            output
        } else {
            let unique_inputs = Self::build_dictionary_cache_input(
                intermediate,
                input_idx,
                dictionary_child,
                dictionary_info.unique_len,
            )?;
            let mut unique_result = Vector::try_new(
                expr.return_type.clone(),
                dictionary_info.unique_len.max(1),
                allocator.clone(),
            )?;
            Self::prepare_result_vector(
                &mut unique_result,
                &expr.return_type,
                dictionary_info.unique_len,
                allocator,
            )?;
            let function_context =
                BoundFunctionContext::new(runtime, expr.function.bind_data.as_deref(), local_state);
            expr.function
                .execute(&unique_inputs, &function_context, &mut unique_result)?;
            let output = Arc::new(unique_result);
            *cached_dictionary_input_id = Some(cache_id);
            *cached_dictionary_output = Some(output.clone());
            output
        };

        let mut result = Vector::try_dictionary(output_child, driving_selection)?;
        result.set_len(count);
        Ok(Some(result))
    }

    fn prepare_in_list(expr: &OperatorExpression) -> Option<PreparedInList> {
        if !matches!(expr.operator_type, OperatorType::In | OperatorType::NotIn)
            || expr.children.len() < 2
        {
            return None;
        }

        let mut values = Vec::with_capacity(expr.children.len().saturating_sub(1));
        let mut has_null = false;
        for child in &expr.children[1..] {
            let Expression::Constant(constant) = child else {
                return Some(PreparedInList::Dynamic);
            };
            if constant.value.is_null() {
                has_null = true;
            } else {
                values.push(constant.value.clone());
            }
        }

        if values.len() > 8 {
            Some(PreparedInList::HashedConst {
                values: values.into_iter().collect::<HashSet<_>>(),
                has_null,
            })
        } else {
            Some(PreparedInList::SmallConst { values, has_null })
        }
    }

    fn select_expression(
        expr: &Expression,
        state: &mut CompiledExpressionState,
        chunk: &Chunk,
        input_sel: Option<&SelectionVector>,
        count: usize,
        runtime: &dyn FunctionExecContext,
        output_sel: &mut SelectionVector,
    ) -> Result<usize> {
        if let Some(selected) =
            Self::try_direct_select(expr, state, chunk, input_sel, count, runtime, output_sel)?
        {
            return Ok(selected);
        }

        let value = Self::execute_value(expr, state, chunk, input_sel, count, runtime)?;
        Ok(scan_bool_selection(
            value.as_vector(),
            input_sel,
            count,
            output_sel,
        ))
    }

    fn try_direct_select(
        expr: &Expression,
        state: &mut CompiledExpressionState,
        chunk: &Chunk,
        input_sel: Option<&SelectionVector>,
        count: usize,
        runtime: &dyn FunctionExecContext,
        output_sel: &mut SelectionVector,
    ) -> Result<Option<usize>> {
        match (expr, state) {
            (Expression::Comparison(expr), CompiledExpressionState::Comparison(state)) => {
                let Some(select) = state.select else {
                    return Ok(None);
                };
                let left = Self::execute_value(
                    &expr.left,
                    &mut state.left,
                    chunk,
                    input_sel,
                    count,
                    runtime,
                )?;
                let right = Self::execute_value(
                    &expr.right,
                    &mut state.right,
                    chunk,
                    input_sel,
                    count,
                    runtime,
                )?;
                Self::store_value(&mut state.left_result, &left);
                Self::store_value(&mut state.right_result, &right);
                Ok(Some(select(
                    left.as_vector(),
                    right.as_vector(),
                    input_sel,
                    count,
                    output_sel,
                )?))
            }
            (Expression::Conjunction(expr), CompiledExpressionState::Conjunction(state)) => {
                if expr.children.is_empty() {
                    return Err(paro_error::internal("Conjunction without children"));
                }

                let allocator = runtime.allocator(MemoryTag::BaseTable);
                let mut current =
                    SelectionVector::try_with_capacity(count.max(1), allocator.clone())?;
                let mut next = SelectionVector::try_with_capacity(count.max(1), allocator)?;

                match expr.conjunction_type {
                    paro_planner::expression::ConjunctionType::And => {
                        let mut current_count = count;
                        for (child_idx, child_expr) in expr.children.iter().enumerate() {
                            let child_input_sel = if child_idx == 0 {
                                input_sel
                            } else {
                                Some(&current)
                            };
                            let selected = Self::select_expression(
                                child_expr,
                                &mut state.child_states[child_idx],
                                chunk,
                                child_input_sel,
                                current_count,
                                runtime,
                                &mut next,
                            )?;
                            std::mem::swap(&mut current, &mut next);
                            current_count = selected;
                            if current_count == 0 {
                                output_sel.set_len(0);
                                return Ok(Some(0));
                            }
                        }
                        Ok(Some(copy_selection(&current, output_sel)))
                    }
                    paro_planner::expression::ConjunctionType::Or => {
                        let mut marks = vec![0; count];
                        for (child_idx, child_expr) in expr.children.iter().enumerate() {
                            Self::select_expression(
                                child_expr,
                                &mut state.child_states[child_idx],
                                chunk,
                                input_sel,
                                count,
                                runtime,
                                &mut next,
                            )?;
                            if child_idx == 0 {
                                super::predicate::mark_selected_rows(
                                    input_sel, count, &next, &mut marks,
                                );
                            } else {
                                accumulate_selected_rows(input_sel, count, &next, &mut marks);
                            }
                        }
                        Ok(Some(build_marked_selection(
                            input_sel, count, &marks, output_sel,
                        )))
                    }
                }
            }
            (Expression::Operator(expr), CompiledExpressionState::Operator(state)) => {
                match expr.operator_type {
                    OperatorType::IsNull => {
                        let child = Self::execute_value(
                            &expr.children[0],
                            &mut state.child_states[0],
                            chunk,
                            input_sel,
                            count,
                            runtime,
                        )?;
                        Self::store_value(&mut state.child_results[0], &child);
                        Ok(Some(scan_null_selection(
                            child.as_vector(),
                            input_sel,
                            count,
                            true,
                            output_sel,
                        )))
                    }
                    OperatorType::IsNotNull => {
                        let child = Self::execute_value(
                            &expr.children[0],
                            &mut state.child_states[0],
                            chunk,
                            input_sel,
                            count,
                            runtime,
                        )?;
                        Self::store_value(&mut state.child_results[0], &child);
                        Ok(Some(scan_null_selection(
                            child.as_vector(),
                            input_sel,
                            count,
                            false,
                            output_sel,
                        )))
                    }
                    OperatorType::Not => {
                        let child = Self::execute_value(
                            &expr.children[0],
                            &mut state.child_states[0],
                            chunk,
                            input_sel,
                            count,
                            runtime,
                        )?;
                        Self::store_value(&mut state.child_results[0], &child);
                        Ok(Some(scan_false_bool_selection(
                            child.as_vector(),
                            input_sel,
                            count,
                            output_sel,
                        )))
                    }
                    _ => Ok(None),
                }
            }
            (Expression::Constant(expr), CompiledExpressionState::Constant(_))
                if expr.return_type == LogicalType::Boolean =>
            {
                let selected = match expr.value {
                    Value::Boolean(true) => select_all_rows(input_sel, count, output_sel),
                    Value::Boolean(false) | Value::Null(_) => {
                        output_sel.set_len(0);
                        0
                    }
                    _ => return Ok(None),
                };
                Ok(Some(selected))
            }
            (Expression::ColumnRef(expr), CompiledExpressionState::ColumnRef(_))
                if expr.return_type == LogicalType::Boolean =>
            {
                let value = Self::execute_column_ref_value(expr, chunk, input_sel, count)?;
                Ok(Some(scan_bool_selection(
                    value.as_vector(),
                    input_sel,
                    count,
                    output_sel,
                )))
            }
            (Expression::Reference(expr), CompiledExpressionState::Reference(_))
                if expr.return_type == LogicalType::Boolean =>
            {
                let value = Self::execute_reference_value(expr, chunk, input_sel, count)?;
                Ok(Some(scan_bool_selection(
                    value.as_vector(),
                    input_sel,
                    count,
                    output_sel,
                )))
            }
            _ => Ok(None),
        }
    }

    fn execute_function_into(
        expr: &FunctionExpression,
        state: &mut ExecuteFunctionState,
        chunk: &Chunk,
        sel: Option<&SelectionVector>,
        count: usize,
        runtime: &dyn FunctionExecContext,
        result: &mut Vector,
    ) -> Result<()> {
        let allocator = runtime.allocator(MemoryTag::BaseTable);
        let ExecuteFunctionState {
            child_states,
            intermediate_types,
            intermediate_chunk,
            local_state,
            cached_dictionary_input_id,
            cached_dictionary_output,
            ..
        } = state;
        let intermediate = Self::prepare_intermediate_chunk(
            intermediate_types,
            intermediate_chunk,
            count,
            allocator.clone(),
        )?;
        for (child_idx, child_expr) in expr.children.iter().enumerate() {
            let child_value = Self::execute_value(
                child_expr,
                &mut child_states[child_idx],
                chunk,
                sel,
                count,
                runtime,
            )?;
            intermediate.data[child_idx] = Arc::new(child_value.as_vector().reference());
        }
        Self::ensure_function_local_state(&expr.function, local_state, runtime)?;
        if let Some(cached_result) = Self::try_dictionary_cached_function(
            expr,
            intermediate,
            count,
            runtime,
            allocator.clone(),
            local_state.as_deref(),
            cached_dictionary_input_id,
            cached_dictionary_output,
        )? {
            *result = cached_result;
            return Ok(());
        }
        Self::prepare_result_vector(result, &expr.return_type, count, allocator)?;
        let function_context = BoundFunctionContext::new(
            runtime,
            expr.function.bind_data.as_deref(),
            local_state.as_deref(),
        );
        expr.function
            .execute(intermediate, &function_context, result)
    }

    fn execute_function_value(
        expr: &FunctionExpression,
        state: &mut ExecuteFunctionState,
        chunk: &Chunk,
        sel: Option<&SelectionVector>,
        count: usize,
        runtime: &dyn FunctionExecContext,
    ) -> Result<EvaluatedValue> {
        let allocator = runtime.allocator(MemoryTag::BaseTable);
        let ExecuteFunctionState {
            child_states,
            intermediate_types,
            intermediate_chunk,
            local_state,
            cached_dictionary_input_id,
            cached_dictionary_output,
            result,
            ..
        } = state;
        let intermediate = Self::prepare_intermediate_chunk(
            intermediate_types,
            intermediate_chunk,
            count,
            allocator.clone(),
        )?;
        for (child_idx, child_expr) in expr.children.iter().enumerate() {
            let child_value = Self::execute_value(
                child_expr,
                &mut child_states[child_idx],
                chunk,
                sel,
                count,
                runtime,
            )?;
            intermediate.data[child_idx] = Arc::new(child_value.as_vector().reference());
        }
        Self::ensure_function_local_state(&expr.function, local_state, runtime)?;
        if let Some(cached_result) = Self::try_dictionary_cached_function(
            expr,
            intermediate,
            count,
            runtime,
            allocator.clone(),
            local_state.as_deref(),
            cached_dictionary_input_id,
            cached_dictionary_output,
        )? {
            return Ok(EvaluatedValue::Borrowed(cached_result));
        }
        let result_vector = Self::prepare_slot_result(result, &expr.return_type, count, allocator)?;
        let function_context = BoundFunctionContext::new(
            runtime,
            expr.function.bind_data.as_deref(),
            local_state.as_deref(),
        );
        expr.function
            .execute(intermediate, &function_context, result_vector)?;
        Ok(result.evaluated(true).expect("function result initialized"))
    }

    fn execute_cast_into(
        expr: &CastExpression,
        state: &mut CastExpressionState,
        chunk: &Chunk,
        sel: Option<&SelectionVector>,
        count: usize,
        runtime: &dyn FunctionExecContext,
        result: &mut Vector,
    ) -> Result<()> {
        let child_value =
            Self::execute_value(&expr.child, &mut state.child, chunk, sel, count, runtime)?;
        Self::store_value(&mut state.child_result, &child_value);

        if child_value.as_vector().logical_type() == &expr.target_type {
            child_value.write_into(result)?;
            result.set_len(count);
            return Ok(());
        }

        Self::prepare_result_vector(
            result,
            &expr.target_type,
            count,
            runtime.allocator(MemoryTag::BaseTable),
        )?;
        let ctx = CastExecCtx {
            runtime,
            try_cast: expr.try_cast,
            cast_data: expr.cast_info.cast_data.as_deref(),
        };
        expr.cast_info
            .execute(child_value.as_vector(), result, count, &ctx)?;
        Ok(())
    }

    fn execute_cast_value(
        expr: &CastExpression,
        state: &mut CastExpressionState,
        chunk: &Chunk,
        sel: Option<&SelectionVector>,
        count: usize,
        runtime: &dyn FunctionExecContext,
    ) -> Result<EvaluatedValue> {
        let child_value =
            Self::execute_value(&expr.child, &mut state.child, chunk, sel, count, runtime)?;
        Self::store_value(&mut state.child_result, &child_value);

        if child_value.as_vector().logical_type() == &expr.target_type {
            return Ok(child_value);
        }

        let allocator = runtime.allocator(MemoryTag::BaseTable);
        let result_vector =
            Self::prepare_slot_result(&mut state.result, &expr.target_type, count, allocator)?;
        let ctx = CastExecCtx {
            runtime,
            try_cast: expr.try_cast,
            cast_data: expr.cast_info.cast_data.as_deref(),
        };
        expr.cast_info
            .execute(child_value.as_vector(), result_vector, count, &ctx)?;
        Ok(state
            .result
            .evaluated(true)
            .expect("cast result initialized"))
    }

    fn execute_comparison_into(
        expr: &ComparisonExpression,
        state: &mut ComparisonExpressionState,
        chunk: &Chunk,
        sel: Option<&SelectionVector>,
        count: usize,
        runtime: &dyn FunctionExecContext,
        result: &mut Vector,
    ) -> Result<()> {
        let left = Self::execute_value(&expr.left, &mut state.left, chunk, sel, count, runtime)?;
        let right = Self::execute_value(&expr.right, &mut state.right, chunk, sel, count, runtime)?;
        Self::store_value(&mut state.left_result, &left);
        Self::store_value(&mut state.right_result, &right);
        Self::prepare_result_vector(
            result,
            &LogicalType::Boolean,
            count,
            runtime.allocator(MemoryTag::BaseTable),
        )?;
        (state.compare)(
            left.as_vector(),
            right.as_vector(),
            result,
            count,
            &COMPARISON_EXEC_CTX,
        )
    }

    fn execute_comparison_value(
        expr: &ComparisonExpression,
        state: &mut ComparisonExpressionState,
        chunk: &Chunk,
        sel: Option<&SelectionVector>,
        count: usize,
        runtime: &dyn FunctionExecContext,
    ) -> Result<EvaluatedValue> {
        let left = Self::execute_value(&expr.left, &mut state.left, chunk, sel, count, runtime)?;
        let right = Self::execute_value(&expr.right, &mut state.right, chunk, sel, count, runtime)?;
        Self::store_value(&mut state.left_result, &left);
        Self::store_value(&mut state.right_result, &right);
        let result = Self::prepare_slot_result(
            &mut state.result,
            &LogicalType::Boolean,
            count,
            runtime.allocator(MemoryTag::BaseTable),
        )?;
        (state.compare)(
            left.as_vector(),
            right.as_vector(),
            result,
            count,
            &COMPARISON_EXEC_CTX,
        )?;
        Ok(state
            .result
            .evaluated(true)
            .expect("comparison result initialized"))
    }

    fn apply_conjunction(
        conjunction_type: paro_planner::expression::ConjunctionType,
        left: &Vector,
        right: &Vector,
        count: usize,
        result: &mut Vector,
    ) {
        use paro_function::scalar::operators::logic::LogicExecutor;

        match conjunction_type {
            paro_planner::expression::ConjunctionType::And => {
                LogicExecutor::execute_and(left, right, result, count);
            }
            paro_planner::expression::ConjunctionType::Or => {
                LogicExecutor::execute_or(left, right, result, count);
            }
        }
    }

    fn execute_conjunction_into(
        expr: &ConjunctionExpression,
        state: &mut ConjunctionExpressionState,
        chunk: &Chunk,
        sel: Option<&SelectionVector>,
        count: usize,
        runtime: &dyn FunctionExecContext,
        result: &mut Vector,
    ) -> Result<()> {
        if expr.children.is_empty() {
            return Err(paro_error::internal("Conjunction without children"));
        }
        let first = Self::execute_value(
            &expr.children[0],
            &mut state.child_states[0],
            chunk,
            sel,
            count,
            runtime,
        )?;
        if expr.children.len() == 1 {
            first.write_into(result)?;
            result.set_len(count);
            return Ok(());
        }

        let allocator = runtime.allocator(MemoryTag::BaseTable);
        let mut current = first.as_vector().reference();
        for child_idx in 1..expr.children.len() {
            let right = Self::execute_value(
                &expr.children[child_idx],
                &mut state.child_states[child_idx],
                chunk,
                sel,
                count,
                runtime,
            )?;
            let target_slot = if child_idx % 2 == 1 {
                &mut state.ping
            } else {
                &mut state.pong
            };
            let target = Self::prepare_slot_result(
                target_slot,
                &LogicalType::Boolean,
                count,
                allocator.clone(),
            )?;
            Self::apply_conjunction(
                expr.conjunction_type,
                &current,
                right.as_vector(),
                count,
                target,
            );
            current = target.reference();
        }
        *result = current;
        result.try_make_exclusive()?;
        Ok(())
    }

    fn execute_conjunction_value(
        expr: &ConjunctionExpression,
        state: &mut ConjunctionExpressionState,
        chunk: &Chunk,
        sel: Option<&SelectionVector>,
        count: usize,
        runtime: &dyn FunctionExecContext,
    ) -> Result<EvaluatedValue> {
        if expr.children.is_empty() {
            return Err(paro_error::internal("Conjunction without children"));
        }
        let first = Self::execute_value(
            &expr.children[0],
            &mut state.child_states[0],
            chunk,
            sel,
            count,
            runtime,
        )?;
        if expr.children.len() == 1 {
            return Ok(first);
        }

        let allocator = runtime.allocator(MemoryTag::BaseTable);
        let mut current = first.as_vector().reference();
        let mut current_is_scratch = false;
        for child_idx in 1..expr.children.len() {
            let right = Self::execute_value(
                &expr.children[child_idx],
                &mut state.child_states[child_idx],
                chunk,
                sel,
                count,
                runtime,
            )?;
            let target_slot = if child_idx % 2 == 1 {
                &mut state.ping
            } else {
                &mut state.pong
            };
            let target = Self::prepare_slot_result(
                target_slot,
                &LogicalType::Boolean,
                count,
                allocator.clone(),
            )?;
            Self::apply_conjunction(
                expr.conjunction_type,
                &current,
                right.as_vector(),
                count,
                target,
            );
            current = target.reference();
            current_is_scratch = true;
        }
        Ok(if current_is_scratch {
            EvaluatedValue::Scratch(current)
        } else {
            EvaluatedValue::Borrowed(current)
        })
    }

    fn execute_case_into(
        expr: &CaseExpression,
        state: &mut CaseExpressionState,
        chunk: &Chunk,
        sel: Option<&SelectionVector>,
        count: usize,
        runtime: &dyn FunctionExecContext,
        result: &mut Vector,
    ) -> Result<()> {
        let check = Self::execute_value(&expr.check, &mut state.check, chunk, sel, count, runtime)?;
        let if_true = Self::execute_value(
            &expr.result_if_true,
            &mut state.result_if_true,
            chunk,
            sel,
            count,
            runtime,
        )?;
        let if_false = Self::execute_value(
            &expr.result_if_false,
            &mut state.result_if_false,
            chunk,
            sel,
            count,
            runtime,
        )?;
        Self::store_value(&mut state.check_result, &check);
        Self::store_value(&mut state.true_result, &if_true);
        Self::store_value(&mut state.false_result, &if_false);
        let merged = paro_function::scalar::operators::case::CaseExecutor::execute(
            check.as_vector(),
            if_true.as_vector(),
            if_false.as_vector(),
            count,
        )?;
        *result = merged;
        Ok(())
    }

    fn execute_case_value(
        expr: &CaseExpression,
        state: &mut CaseExpressionState,
        chunk: &Chunk,
        sel: Option<&SelectionVector>,
        count: usize,
        runtime: &dyn FunctionExecContext,
    ) -> Result<EvaluatedValue> {
        let check = Self::execute_value(&expr.check, &mut state.check, chunk, sel, count, runtime)?;
        let if_true = Self::execute_value(
            &expr.result_if_true,
            &mut state.result_if_true,
            chunk,
            sel,
            count,
            runtime,
        )?;
        let if_false = Self::execute_value(
            &expr.result_if_false,
            &mut state.result_if_false,
            chunk,
            sel,
            count,
            runtime,
        )?;
        Self::store_value(&mut state.check_result, &check);
        Self::store_value(&mut state.true_result, &if_true);
        Self::store_value(&mut state.false_result, &if_false);
        state.result.set_value(
            paro_function::scalar::operators::case::CaseExecutor::execute(
                check.as_vector(),
                if_true.as_vector(),
                if_false.as_vector(),
                count,
            )?,
        );
        Ok(state
            .result
            .evaluated(true)
            .expect("case result initialized"))
    }

    fn execute_operator_into(
        expr: &OperatorExpression,
        state: &mut OperatorExpressionState,
        chunk: &Chunk,
        sel: Option<&SelectionVector>,
        count: usize,
        runtime: &dyn FunctionExecContext,
        result: &mut Vector,
    ) -> Result<()> {
        let value = Self::execute_operator_value(expr, state, chunk, sel, count, runtime)?;
        value.write_into(result)?;
        result.set_len(count);
        Ok(())
    }

    fn execute_operator_value(
        expr: &OperatorExpression,
        state: &mut OperatorExpressionState,
        chunk: &Chunk,
        sel: Option<&SelectionVector>,
        count: usize,
        runtime: &dyn FunctionExecContext,
    ) -> Result<EvaluatedValue> {
        match expr.operator_type {
            OperatorType::Not => {
                let child = Self::execute_value(
                    &expr.children[0],
                    &mut state.child_states[0],
                    chunk,
                    sel,
                    count,
                    runtime,
                )?;
                Self::store_value(&mut state.child_results[0], &child);
                let result = Self::prepare_slot_result(
                    &mut state.result,
                    &LogicalType::Boolean,
                    count,
                    runtime.allocator(MemoryTag::BaseTable),
                )?;
                use paro_function::scalar::executor::unary::UnaryExecutor;
                use paro_function::scalar::operators::logic::NotOperator;
                UnaryExecutor::execute::<bool, bool, NotOperator>(
                    child.as_vector(),
                    result,
                    count,
                )?;
                Ok(state
                    .result
                    .evaluated(true)
                    .expect("not result initialized"))
            }
            OperatorType::IsNull | OperatorType::IsNotNull => {
                let child = Self::execute_value(
                    &expr.children[0],
                    &mut state.child_states[0],
                    chunk,
                    sel,
                    count,
                    runtime,
                )?;
                Self::store_value(&mut state.child_results[0], &child);
                let result = Self::prepare_slot_result(
                    &mut state.result,
                    &LogicalType::Boolean,
                    count,
                    runtime.allocator(MemoryTag::BaseTable),
                )?;
                let is_not = matches!(expr.operator_type, OperatorType::IsNotNull);
                for row_idx in 0..count {
                    result.set_bool(row_idx, child.as_vector().is_null(row_idx) != is_not);
                }
                Ok(state
                    .result
                    .evaluated(true)
                    .expect("is null result initialized"))
            }
            OperatorType::Coalesce => {
                let result = Self::prepare_slot_result(
                    &mut state.result,
                    &expr.return_type,
                    count,
                    runtime.allocator(MemoryTag::BaseTable),
                )?;

                for row_idx in 0..count {
                    result.set_null(row_idx, true);
                }

                let allocator = runtime.allocator(MemoryTag::BaseTable);
                let mut unresolved =
                    SelectionVector::try_with_capacity(count.max(1), allocator.clone())?;
                let mut next_unresolved =
                    SelectionVector::try_with_capacity(count.max(1), allocator)?;
                let mut child_sel = SelectionVector::try_with_capacity(
                    count.max(1),
                    runtime.allocator(MemoryTag::BaseTable),
                )?;
                let mut unresolved_count = select_all_rows(None, count, &mut unresolved);

                for (child_idx, child_expr) in expr.children.iter().enumerate() {
                    if unresolved_count == 0 {
                        break;
                    }

                    let child_input_sel = if let Some(base_sel) = sel {
                        child_sel.set_len(unresolved_count);
                        for row_idx in 0..unresolved_count {
                            child_sel.set(row_idx, base_sel.get(unresolved.get(row_idx)));
                        }
                        child_sel.set_len(unresolved_count);
                        Some(&child_sel)
                    } else {
                        Some(&unresolved)
                    };

                    let child = Self::execute_value(
                        child_expr,
                        &mut state.child_states[child_idx],
                        chunk,
                        child_input_sel,
                        unresolved_count,
                        runtime,
                    )?;
                    Self::store_value(&mut state.child_results[child_idx], &child);

                    let next_count = apply_coalesce_child(
                        result,
                        child.as_vector(),
                        &unresolved,
                        unresolved_count,
                        &mut next_unresolved,
                    );
                    std::mem::swap(&mut unresolved, &mut next_unresolved);
                    unresolved_count = next_count;
                }

                Ok(state
                    .result
                    .evaluated(true)
                    .expect("coalesce result initialized"))
            }
            OperatorType::In | OperatorType::NotIn => {
                use paro_function::scalar::executor::unary::UnaryExecutor;
                use paro_function::scalar::operators::logic::NotOperator;

                let lhs = Self::execute_value(
                    &expr.children[0],
                    &mut state.child_states[0],
                    chunk,
                    sel,
                    count,
                    runtime,
                )?;
                Self::store_value(&mut state.child_results[0], &lhs);

                let negate = matches!(expr.operator_type, OperatorType::NotIn);
                let result = Self::prepare_slot_result(
                    &mut state.result,
                    &LogicalType::Boolean,
                    count,
                    runtime.allocator(MemoryTag::BaseTable),
                )?;

                match state
                    .in_list
                    .as_ref()
                    .expect("IN operator should initialize an execution strategy")
                {
                    PreparedInList::SmallConst { values, has_null } => {
                        Self::execute_in_const_values(
                            lhs.as_vector(),
                            values,
                            *has_null,
                            negate,
                            result,
                            count,
                        );
                    }
                    PreparedInList::HashedConst { values, has_null } => {
                        Self::execute_in_const_hash(
                            lhs.as_vector(),
                            values,
                            *has_null,
                            negate,
                            result,
                            count,
                        );
                    }
                    PreparedInList::Dynamic => {
                        let eq_dispatch = compile_comparison_dispatch(
                            lhs.as_vector().logical_type(),
                            paro_planner::expression::ComparisonType::Equal,
                        );
                        let comp_result = Self::prepare_slot_result(
                            &mut state.aux,
                            &LogicalType::Boolean,
                            count,
                            runtime.allocator(MemoryTag::BaseTable),
                        )?;
                        let scratch = Self::prepare_slot_result(
                            &mut state.scratch,
                            &LogicalType::Boolean,
                            count,
                            runtime.allocator(MemoryTag::BaseTable),
                        )?;
                        *result = Vector::try_constant::<bool>(
                            LogicalType::Boolean,
                            false,
                            count,
                            runtime.allocator(MemoryTag::BaseTable),
                        )?;

                        for rhs_idx in 1..expr.children.len() {
                            let rhs = Self::execute_value(
                                &expr.children[rhs_idx],
                                &mut state.child_states[rhs_idx],
                                chunk,
                                sel,
                                count,
                                runtime,
                            )?;
                            Self::store_value(&mut state.child_results[rhs_idx], &rhs);
                            (eq_dispatch.compare)(
                                lhs.as_vector(),
                                rhs.as_vector(),
                                comp_result,
                                count,
                                &COMPARISON_EXEC_CTX,
                            )?;
                            LogicExecutor::execute_or(result, comp_result, scratch, count);
                            std::mem::swap(result, scratch);
                        }

                        if negate {
                            UnaryExecutor::execute::<bool, bool, NotOperator>(
                                result, scratch, count,
                            )?;
                            std::mem::swap(result, scratch);
                        }
                    }
                }

                Ok(state.result.evaluated(true).expect("in result initialized"))
            }
            OperatorType::ArrayConstructor => {
                let array_size = expr.children.len();
                let mut result = Vector::try_new_array(
                    expr.return_type.clone(),
                    count,
                    runtime.allocator(MemoryTag::BaseTable),
                )?;
                result.set_count(count);

                let child_vec = result.child_mut().ok_or_else(|| {
                    paro_error::internal("Array constructor missing child vector")
                })?;
                let child_vec = Arc::make_mut(child_vec);

                for (child_idx, child_expr) in expr.children.iter().enumerate() {
                    let child = Self::execute_value(
                        child_expr,
                        &mut state.child_states[child_idx],
                        chunk,
                        sel,
                        count,
                        runtime,
                    )?;
                    Self::store_value(&mut state.child_results[child_idx], &child);
                    for row in 0..count {
                        let offset = row * array_size + child_idx;
                        if child.as_vector().is_null(row) {
                            child_vec.set_null(offset, true);
                        } else {
                            let value = child.as_vector().get_value(row);
                            child_vec.set_value(offset, &value);
                        }
                    }
                }

                for row in 0..count {
                    result.set_null(row, false);
                }
                state.result.set_value(result);
                Ok(state
                    .result
                    .evaluated(true)
                    .expect("array constructor initialized"))
            }
            OperatorType::StructConstructor => {
                let mut result = Vector::try_new(
                    expr.return_type.clone(),
                    count.max(1),
                    runtime.allocator(MemoryTag::BaseTable),
                )?;
                result.set_count(count);

                let children = result
                    .children_mut()
                    .ok_or_else(|| paro_error::internal("Struct constructor missing children"))?;

                for (child_idx, child_expr) in expr.children.iter().enumerate() {
                    let child = Self::execute_value(
                        child_expr,
                        &mut state.child_states[child_idx],
                        chunk,
                        sel,
                        count,
                        runtime,
                    )?;
                    Self::store_value(&mut state.child_results[child_idx], &child);
                    let child_vec = Arc::make_mut(&mut children[child_idx]);
                    for row in 0..count {
                        if child.as_vector().is_null(row) {
                            child_vec.set_null(row, true);
                        } else {
                            let value = child.as_vector().get_value(row);
                            child_vec.set_value(row, &value);
                        }
                    }
                }

                for row in 0..count {
                    result.set_null(row, false);
                }
                state.result.set_value(result);
                Ok(state
                    .result
                    .evaluated(true)
                    .expect("struct constructor initialized"))
            }
            OperatorType::ErrorIfMultipleRows => {
                let value = Self::execute_value(
                    &expr.children[0],
                    &mut state.child_states[0],
                    chunk,
                    sel,
                    count,
                    runtime,
                )?;
                let row_count = Self::execute_value(
                    &expr.children[1],
                    &mut state.child_states[1],
                    chunk,
                    sel,
                    count,
                    runtime,
                )?;
                Self::store_value(&mut state.child_results[0], &value);
                Self::store_value(&mut state.child_results[1], &row_count);

                for row_idx in 0..count {
                    if row_count.as_vector().is_null(row_idx) {
                        continue;
                    }
                    let rows = row_count
                        .as_vector()
                        .get_value(row_idx)
                        .as_i64()
                        .ok_or_else(|| {
                            paro_error::internal(
                                "Scalar subquery row-count check expected BIGINT count value",
                            )
                        })?;
                    if rows > 1 {
                        return Err(paro_error::syntax(
                            "More than one row returned by a subquery used as an expression",
                        ));
                    }
                }

                Ok(value)
            }
            OperatorType::Like | OperatorType::ILike | OperatorType::ArrayExtract => Err(
                paro_error::not_implemented(format!("{:?} operator", expr.operator_type)),
            ),
        }
    }

    fn execute_in_const_values(
        lhs: &Vector,
        values: &[Value],
        has_null_rhs: bool,
        negate: bool,
        result: &mut Vector,
        count: usize,
    ) {
        for row_idx in 0..count {
            let lhs_value = lhs.get_value(row_idx);
            if lhs_value.is_null() {
                result.set_null(row_idx, true);
                continue;
            }

            let mut matched = false;
            for value in values {
                if lhs_value == *value {
                    matched = true;
                    break;
                }
            }

            if matched {
                result.set_bool(row_idx, !negate);
            } else if has_null_rhs {
                result.set_null(row_idx, true);
            } else {
                result.set_bool(row_idx, negate);
            }
        }
    }

    fn execute_in_const_hash(
        lhs: &Vector,
        values: &HashSet<Value>,
        has_null_rhs: bool,
        negate: bool,
        result: &mut Vector,
        count: usize,
    ) {
        for row_idx in 0..count {
            let lhs_value = lhs.get_value(row_idx);
            if lhs_value.is_null() {
                result.set_null(row_idx, true);
                continue;
            }

            if values.contains(&lhs_value) {
                result.set_bool(row_idx, !negate);
            } else if has_null_rhs {
                result.set_null(row_idx, true);
            } else {
                result.set_bool(row_idx, negate);
            }
        }
    }

    fn execute_constant_into(
        expr: &ConstantExpression,
        count: usize,
        runtime: &dyn FunctionExecContext,
        result: &mut Vector,
    ) -> Result<()> {
        *result = Vector::try_constant_from_value(
            expr.return_type.clone(),
            expr.value.clone(),
            count,
            runtime.allocator(MemoryTag::BaseTable),
        )?;
        Ok(())
    }

    fn execute_column_ref_value(
        expr: &ColumnRefExpression,
        chunk: &Chunk,
        sel: Option<&SelectionVector>,
        count: usize,
    ) -> Result<EvaluatedValue> {
        if expr.binding.column_index >= chunk.data.len() {
            if count == 0 {
                return Ok(EvaluatedValue::Borrowed(Vector::try_new(
                    expr.return_type.clone(),
                    0,
                    chunk.allocator().clone(),
                )?));
            }
            return Err(paro_error::internal(format!(
                "Column reference index {} out of bounds (chunk columns={})",
                expr.binding.column_index,
                chunk.data.len()
            )));
        }
        let column = chunk.data[expr.binding.column_index].as_ref();
        if let Some(sel) = sel {
            Ok(EvaluatedValue::Borrowed(Vector::try_dictionary(
                chunk.data[expr.binding.column_index].clone(),
                sel,
            )?))
        } else {
            Ok(EvaluatedValue::Borrowed(column.reference()))
        }
    }

    fn execute_column_ref_into(
        expr: &ColumnRefExpression,
        chunk: &Chunk,
        sel: Option<&SelectionVector>,
        count: usize,
        result: &mut Vector,
    ) -> Result<()> {
        let value = Self::execute_column_ref_value(expr, chunk, sel, count)?;
        value.write_into(result)?;
        result.set_len(count);
        Ok(())
    }

    fn execute_reference_value(
        expr: &ReferenceExpression,
        chunk: &Chunk,
        sel: Option<&SelectionVector>,
        count: usize,
    ) -> Result<EvaluatedValue> {
        if expr.index >= chunk.data.len() {
            if count == 0 {
                return Ok(EvaluatedValue::Borrowed(Vector::try_new(
                    expr.return_type.clone(),
                    0,
                    chunk.allocator().clone(),
                )?));
            }
            return Err(paro_error::internal(format!(
                "Reference index {} out of bounds (chunk columns={})",
                expr.index,
                chunk.data.len()
            )));
        }
        let column = chunk.data[expr.index].as_ref();
        if let Some(sel) = sel {
            Ok(EvaluatedValue::Borrowed(Vector::try_dictionary(
                chunk.data[expr.index].clone(),
                sel,
            )?))
        } else {
            Ok(EvaluatedValue::Borrowed(column.reference()))
        }
    }

    fn execute_reference_into(
        expr: &ReferenceExpression,
        chunk: &Chunk,
        sel: Option<&SelectionVector>,
        count: usize,
        result: &mut Vector,
    ) -> Result<()> {
        let value = Self::execute_reference_value(expr, chunk, sel, count)?;
        value.write_into(result)?;
        result.set_len(count);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::any::Any;
    use std::panic::{catch_unwind, AssertUnwindSafe};
    use std::ptr;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use super::*;
    use crate::execution_context::ExecutionContext;
    use crate::thread_context::ThreadContext;
    use paro_common::vector::{DictionaryInfo, DictionarySource, VectorType};
    use paro_context::{test_support::TestStatementContextBuilder, StatementContext};
    use paro_function::scalar::cast::{numeric_casts, BoundCastInfo};
    use paro_function::scalar::{
        BoundScalarFunction, DictionaryStrategy, FunctionData, FunctionErrorMode,
        FunctionLocalState, ScalarFunction,
    };
    use paro_planner::binder::context::BindContext;
    use paro_planner::expression::{
        CastExpression, ColumnRefExpression, ComparisonExpression, ComparisonType,
        ConjunctionExpression, ConjunctionType, ConstantExpression, FunctionExpression,
        OperatorExpression, OperatorType, ReferenceExpression, SubqueryExpression,
        SubqueryPlanningState, SubqueryType,
    };
    use paro_planner::operator::ColumnBinding;
    use paro_planner::operator::{ExpressionGet, LogicalOperator};
    use paro_planner::plan::{LogicalPlan, PlannedStatement};

    static LOCAL_STATE_INIT_COUNT: AtomicUsize = AtomicUsize::new(0);

    fn test_session() -> Arc<StatementContext> {
        TestStatementContextBuilder::minimal().build()
    }

    fn test_runtime(session: Arc<StatementContext>) -> ExecutionContext<'static> {
        let thread = Box::leak(Box::new(ThreadContext::single_threaded()));
        ExecutionContext::new(session, thread, None)
    }

    fn scalar_subquery_check_expr(row_count: i64) -> Expression {
        Expression::Operator(OperatorExpression::new(
            OperatorType::ErrorIfMultipleRows,
            vec![
                Expression::Constant(ConstantExpression {
                    value: Value::Integer(42),
                    return_type: LogicalType::Integer,
                }),
                Expression::Constant(ConstantExpression {
                    value: Value::BigInt(row_count),
                    return_type: LogicalType::BigInt,
                }),
            ],
            LogicalType::Integer,
        ))
    }

    fn unflattened_subquery_expr() -> Expression {
        Expression::Subquery(SubqueryExpression {
            subquery_type: SubqueryType::Scalar,
            subquery: Arc::new(PlannedStatement {
                types: vec![LogicalType::Integer],
                names: vec!["v".to_string()],
                plan: LogicalPlan::new(
                    &BindContext::new(),
                    LogicalOperator::ExpressionGet(ExpressionGet::new(
                        99,
                        vec![],
                        vec!["v".to_string()],
                        vec![LogicalType::Integer],
                    )),
                ),
            }),
            children: vec![],
            child_types: vec![],
            child_targets: vec![],
            comparison_type: paro_planner::expression::ComparisonType::Equal,
            return_type: LogicalType::Integer,
            correlated_columns: vec![],
            bind_snapshot: BindContext::new().snapshot(),
            planning_state: SubqueryPlanningState::Unplanned,
        })
    }

    fn integer_chunk(values: &[i32]) -> Chunk {
        Chunk::from_vectors(
            vec![paro_common::test_utils::test_i32_vector_with_allocator(
                values,
                paro_common::test_utils::test_allocator(),
            )],
            paro_common::test_utils::test_allocator(),
        )
    }

    fn nullable_i32_vector(values: &[Option<i32>]) -> Vector {
        let dense: Vec<i32> = values
            .iter()
            .map(|value| value.unwrap_or_default())
            .collect();
        let mut vector = paro_common::test_utils::test_i32_vector_with_allocator(
            &dense,
            paro_common::test_utils::test_allocator(),
        );
        for (row_idx, value) in values.iter().enumerate() {
            if value.is_none() {
                vector.set_null(row_idx, true);
            }
        }
        vector
    }

    fn nullable_integer_chunk(values: &[Option<i32>]) -> Chunk {
        Chunk::from_vectors(
            vec![nullable_i32_vector(values)],
            paro_common::test_utils::test_allocator(),
        )
    }

    fn boolean_chunk(values: &[Option<bool>]) -> Chunk {
        Chunk::from_vectors(
            vec![paro_common::test_utils::test_nullable_bool_vector(values)],
            paro_common::test_utils::test_allocator(),
        )
    }

    fn vector_from_i64_values(logical_type: LogicalType, values: &[i64]) -> Vector {
        let mut vector =
            paro_common::test_utils::test_vector_with_capacity(logical_type, values.len());
        vector.set_count(values.len());
        unsafe {
            ptr::copy_nonoverlapping(values.as_ptr(), vector.flat_data_mut::<i64>(), values.len());
        }
        vector
    }

    fn constant_i32(value: i32) -> Expression {
        Expression::Constant(ConstantExpression::new(
            Value::Integer(value),
            LogicalType::Integer,
        ))
    }

    fn reference_i32(index: usize) -> Expression {
        Expression::Reference(ReferenceExpression::new(index, LogicalType::Integer))
    }

    fn reference_i64(index: usize) -> Expression {
        Expression::Reference(ReferenceExpression::new(index, LogicalType::BigInt))
    }

    fn reference_timestamp(index: usize) -> Expression {
        Expression::Reference(ReferenceExpression::new(index, LogicalType::Timestamp))
    }

    fn cast_expr(
        child: Expression,
        target_type: LogicalType,
        cast_info: BoundCastInfo,
        try_cast: bool,
    ) -> Expression {
        Expression::Cast(CastExpression::new(child, target_type, cast_info, try_cast))
    }

    fn greater_than_i32(index: usize, value: i32) -> Expression {
        Expression::Comparison(ComparisonExpression::new(
            ComparisonType::GreaterThan,
            reference_i32(index),
            constant_i32(value),
        ))
    }

    fn less_than_i32(index: usize, value: i32) -> Expression {
        Expression::Comparison(ComparisonExpression::new(
            ComparisonType::LessThan,
            reference_i32(index),
            constant_i32(value),
        ))
    }

    fn null_i32() -> Expression {
        Expression::Constant(ConstantExpression::new(
            Value::Null(LogicalType::Integer),
            LogicalType::Integer,
        ))
    }

    fn coalesce_i32_expr(children: Vec<Expression>) -> Expression {
        Expression::Operator(OperatorExpression::new(
            OperatorType::Coalesce,
            children,
            LogicalType::Integer,
        ))
    }

    fn in_list_i32_expr(values: &[Option<i32>], not: bool) -> Expression {
        let mut children = vec![reference_i32(0)];
        for value in values {
            let expr = match value {
                Some(value) => constant_i32(*value),
                None => null_i32(),
            };
            children.push(expr);
        }
        Expression::Operator(OperatorExpression::new(
            if not {
                OperatorType::NotIn
            } else {
                OperatorType::In
            },
            children,
            LogicalType::Boolean,
        ))
    }

    fn reference_bool(index: usize) -> Expression {
        Expression::Reference(ReferenceExpression::new(index, LogicalType::Boolean))
    }

    fn add_one_function(
        input: &Chunk,
        _runtime: &dyn FunctionExecContext,
        result: &mut Vector,
    ) -> Result<()> {
        let column = input
            .column(0)
            .expect("add_one input should expose its first column");
        for row_idx in 0..input.size() {
            result.set_i32(
                row_idx,
                column
                    .get_i32(row_idx)
                    .expect("add_one expects non-null integer input")
                    + 1,
            );
        }
        Ok(())
    }

    fn add_one_expr(index: usize) -> Expression {
        let function = ScalarFunction::new(
            "add_one".to_string(),
            vec![LogicalType::Integer],
            LogicalType::Integer,
            add_one_function,
        );
        Expression::Function(FunctionExpression::new(
            function,
            vec![reference_i32(index)],
            LogicalType::Integer,
        ))
    }

    #[derive(Debug, Clone, PartialEq)]
    struct OffsetBindData {
        offset: i32,
    }

    impl FunctionData for OffsetBindData {
        fn clone_box(&self) -> Box<dyn FunctionData> {
            Box::new(self.clone())
        }

        fn equals(&self, other: &dyn FunctionData) -> bool {
            other
                .as_any()
                .downcast_ref::<Self>()
                .is_some_and(|other| other == self)
        }

        fn as_any(&self) -> &dyn Any {
            self
        }
    }

    #[derive(Debug)]
    struct OffsetLocalState {
        offset: i32,
    }

    impl FunctionLocalState for OffsetLocalState {
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn as_any_mut(&mut self) -> &mut dyn Any {
            self
        }
    }

    fn init_offset_local_state(
        _runtime: &dyn FunctionExecContext,
        bind_data: Option<&dyn FunctionData>,
    ) -> Result<Box<dyn FunctionLocalState>> {
        LOCAL_STATE_INIT_COUNT.fetch_add(1, Ordering::SeqCst);
        let offset = bind_data
            .and_then(|data| data.as_any().downcast_ref::<OffsetBindData>())
            .map(|data| data.offset)
            .unwrap_or_default();
        Ok(Box::new(OffsetLocalState { offset }))
    }

    fn add_with_local_state(
        input: &Chunk,
        runtime: &dyn FunctionExecContext,
        result: &mut Vector,
    ) -> Result<()> {
        let column = input.column(0).expect("input column");
        let offset = runtime
            .local_state()
            .and_then(|state| state.as_any().downcast_ref::<OffsetLocalState>())
            .map(|state| state.offset)
            .expect("local state initialized");

        for row_idx in 0..input.size() {
            result.set_i32(
                row_idx,
                column.get_i32(row_idx).expect("integer input") + offset,
            );
        }
        Ok(())
    }

    fn add_with_local_state_expr(index: usize) -> Expression {
        let function = BoundScalarFunction::from(ScalarFunction::new(
            "add_with_local_state".to_string(),
            vec![LogicalType::Integer],
            LogicalType::Integer,
            add_with_local_state,
        ))
        .with_bind_data(OffsetBindData { offset: 7 })
        .with_init_local_state(init_offset_local_state);
        Expression::Function(FunctionExpression::new(
            function,
            vec![reference_i32(index)],
            LogicalType::Integer,
        ))
    }

    #[derive(Debug, Clone)]
    struct ExecutionCounterData {
        counter: Arc<AtomicUsize>,
    }

    impl FunctionData for ExecutionCounterData {
        fn clone_box(&self) -> Box<dyn FunctionData> {
            Box::new(self.clone())
        }

        fn equals(&self, other: &dyn FunctionData) -> bool {
            other
                .as_any()
                .downcast_ref::<Self>()
                .is_some_and(|other| Arc::ptr_eq(&self.counter, &other.counter))
        }

        fn as_any(&self) -> &dyn Any {
            self
        }
    }

    fn bump_execution_counter(runtime: &dyn FunctionExecContext) {
        runtime
            .bind_data()
            .and_then(|data| data.as_any().downcast_ref::<ExecutionCounterData>())
            .expect("execution counter bind data should exist")
            .counter
            .fetch_add(1, Ordering::SeqCst);
    }

    fn counted_identity_function(
        input: &Chunk,
        runtime: &dyn FunctionExecContext,
        result: &mut Vector,
    ) -> Result<()> {
        bump_execution_counter(runtime);
        let column = input.column(0).expect("counted identity input");
        for row_idx in 0..input.size() {
            if column.is_null(row_idx) {
                result.set_null(row_idx, true);
            } else {
                result.set_i32(
                    row_idx,
                    column
                        .get_i32(row_idx)
                        .expect("counted identity integer input"),
                );
            }
        }
        Ok(())
    }

    fn counted_add_pair_function(
        input: &Chunk,
        runtime: &dyn FunctionExecContext,
        result: &mut Vector,
    ) -> Result<()> {
        bump_execution_counter(runtime);
        let left = input.column(0).expect("counted add left input");
        let right = input.column(1).expect("counted add right input");
        for row_idx in 0..input.size() {
            result.set_i32(
                row_idx,
                left.get_i32(row_idx)
                    .expect("counted add left integer input")
                    + right
                        .get_i32(row_idx)
                        .expect("counted add right integer input"),
            );
        }
        Ok(())
    }

    fn cached_identity_expr(index: usize) -> (Expression, Arc<AtomicUsize>) {
        let counter = Arc::new(AtomicUsize::new(0));
        let function = BoundScalarFunction::from(ScalarFunction::new(
            "counted_identity".to_string(),
            vec![LogicalType::Integer],
            LogicalType::Integer,
            counted_identity_function,
        ))
        .with_bind_data(ExecutionCounterData {
            counter: counter.clone(),
        })
        .with_error_mode(FunctionErrorMode::Infallible)
        .with_dictionary_strategy(DictionaryStrategy::StorageDictionaryCache { input_idx: 0 });
        (
            Expression::Function(FunctionExpression::new(
                function,
                vec![reference_i32(index)],
                LogicalType::Integer,
            )),
            counter,
        )
    }

    fn cached_add_pair_expr(
        left_index: usize,
        right_index: usize,
    ) -> (Expression, Arc<AtomicUsize>) {
        let counter = Arc::new(AtomicUsize::new(0));
        let function = BoundScalarFunction::from(ScalarFunction::new(
            "counted_add_pair".to_string(),
            vec![LogicalType::Integer, LogicalType::Integer],
            LogicalType::Integer,
            counted_add_pair_function,
        ))
        .with_bind_data(ExecutionCounterData {
            counter: counter.clone(),
        })
        .with_error_mode(FunctionErrorMode::Infallible)
        .with_dictionary_strategy(DictionaryStrategy::StorageDictionaryCache { input_idx: 0 });
        (
            Expression::Function(FunctionExpression::new(
                function,
                vec![reference_i32(left_index), reference_i32(right_index)],
                LogicalType::Integer,
            )),
            counter,
        )
    }

    fn storage_dictionary_i32(values: &[i32], selection: Vec<u32>, provenance_id: u64) -> Vector {
        paro_common::test_utils::test_with_dictionary(
            Arc::new(paro_common::test_utils::test_i32_vector_with_allocator(
                values,
                paro_common::test_utils::test_allocator(),
            )),
            selection,
            DictionaryInfo {
                unique_len: values.len(),
                provenance_id: Some(provenance_id),
                source: DictionarySource::Storage,
            },
        )
    }

    #[test]
    fn error_if_multiple_rows_returns_value_for_single_row() {
        let session = test_session();
        let runtime = test_runtime(session.clone());
        let expr = scalar_subquery_check_expr(1);
        let mut executor = ExpressionExecutor::new(&expr);
        let mut input = Chunk::try_new(paro_common::test_utils::test_allocator())
            .expect("test chunk allocation failed");
        input.set_cardinality(1);

        let result = executor
            .execute_expression(0, &input, None, input.size(), &runtime)
            .expect("execute");

        assert_eq!(result.get_i32(0), Some(42));
    }

    #[test]
    fn error_if_multiple_rows_errors_for_duplicate_rows() {
        let session = test_session();
        let runtime = test_runtime(session.clone());
        let expr = scalar_subquery_check_expr(2);
        let mut executor = ExpressionExecutor::new(&expr);
        let mut input = Chunk::try_new(paro_common::test_utils::test_allocator())
            .expect("test chunk allocation failed");
        input.set_cardinality(1);

        let err = executor
            .execute_expression(0, &input, None, input.size(), &runtime)
            .expect_err("expected scalar subquery error");

        assert!(err.to_string().contains("More than one row returned"));
    }

    #[test]
    fn expression_executor_rejects_unflattened_subquery_expression() {
        let expr = unflattened_subquery_expr();

        let panic = catch_unwind(AssertUnwindSafe(|| {
            let _ = ExpressionExecutor::new(&expr);
        }))
        .expect_err("executor should panic on subquery expressions");

        let message = if let Some(message) = panic.downcast_ref::<String>() {
            message.clone()
        } else if let Some(message) = panic.downcast_ref::<&str>() {
            message.to_string()
        } else {
            String::new()
        };
        assert!(message.contains("Expression::Subquery"));
    }

    #[test]
    fn select_into_writes_selection_vector() {
        let session = test_session();
        let runtime = test_runtime(session);
        let expr = greater_than_i32(0, 1);
        let mut executor = ExpressionExecutor::new(&expr);
        let input = integer_chunk(&[0, 2, 1, 5]);
        let mut selection = paro_common::test_utils::test_selection_with_capacity(input.size());
        selection.set_len(input.size());

        let selected = executor
            .select_into(0, &input, input.size(), &runtime, &mut selection)
            .expect("select_into should succeed");

        assert_eq!(selected, 2);
        assert_eq!(selection.as_slice(), &[1, 3]);
    }

    #[test]
    fn comparison_select_path_avoids_materializing_boolean_result() {
        let session = test_session();
        let runtime = test_runtime(session);
        let expr = greater_than_i32(0, 1);
        let mut executor = ExpressionExecutor::new(&expr);
        let input = integer_chunk(&[0, 2, 1, 5]);
        let mut selection = paro_common::test_utils::test_selection_with_capacity(input.size());

        let selected = executor
            .select_into(0, &input, input.size(), &runtime, &mut selection)
            .expect("comparison select should succeed");

        assert_eq!(selected, 2);
        assert_eq!(selection.as_slice(), &[1, 3]);
        match executor.compiled_state(0) {
            CompiledExpressionState::Comparison(state) => {
                assert!(state.result.as_ref().is_none());
            }
            other => panic!("expected comparison state, got {other:?}"),
        }
    }

    #[test]
    fn conjunction_select_path_intersects_child_predicates_without_bool_materialization() {
        let session = test_session();
        let runtime = test_runtime(session);
        let expr = Expression::Conjunction(ConjunctionExpression::new(
            ConjunctionType::And,
            vec![greater_than_i32(0, 1), less_than_i32(0, 4)],
        ));
        let mut executor = ExpressionExecutor::new(&expr);
        let input = integer_chunk(&[0, 2, 3, 5]);
        let mut selection = paro_common::test_utils::test_selection_with_capacity(input.size());

        let selected = executor
            .select_into(0, &input, input.size(), &runtime, &mut selection)
            .expect("conjunction select should succeed");

        assert_eq!(selected, 2);
        assert_eq!(selection.as_slice(), &[1, 2]);
        match executor.compiled_state(0) {
            CompiledExpressionState::Conjunction(state) => {
                for child_state in &state.child_states {
                    if let CompiledExpressionState::Comparison(child) = child_state {
                        assert!(child.result.as_ref().is_none());
                    }
                }
            }
            other => panic!("expected conjunction state, got {other:?}"),
        }
    }

    #[test]
    fn distinct_from_truth_table_matches_sql_null_semantics() {
        let session = test_session();
        let runtime = test_runtime(session);
        let expr = Expression::Comparison(ComparisonExpression::new(
            ComparisonType::DistinctFrom,
            reference_i32(0),
            reference_i32(1),
        ));
        let mut executor = ExpressionExecutor::new(&expr);
        let input = Chunk::from_vectors(
            vec![
                nullable_i32_vector(&[Some(1), Some(1), Some(1), None, None]),
                nullable_i32_vector(&[Some(1), Some(2), None, Some(1), None]),
            ],
            paro_common::test_utils::test_allocator(),
        );
        let mut result =
            paro_common::test_utils::test_vector_with_capacity(LogicalType::Boolean, input.size());

        executor
            .execute_into(0, &input, None, input.size(), &runtime, &mut result)
            .expect("distinct from should execute");

        assert_eq!(result.get_bool(0), Some(false));
        assert_eq!(result.get_bool(1), Some(true));
        assert_eq!(result.get_bool(2), Some(true));
        assert_eq!(result.get_bool(3), Some(true));
        assert_eq!(result.get_bool(4), Some(false));
    }

    #[test]
    fn coalesce_selection_shrinking_respects_selection_overlay() {
        let session = test_session();
        let runtime = test_runtime(session);
        let expr = coalesce_i32_expr(vec![reference_i32(0), constant_i32(99)]);
        let mut executor = ExpressionExecutor::new(&expr);
        let input = nullable_integer_chunk(&[None, Some(7), None, Some(5)]);
        let selection = paro_common::test_utils::test_selection(vec![3, 0, 1]);
        let mut result = paro_common::test_utils::test_vector_with_capacity(
            LogicalType::Integer,
            selection.len(),
        );

        executor
            .execute_into(
                0,
                &input,
                Some(&selection),
                selection.len(),
                &runtime,
                &mut result,
            )
            .expect("coalesce over selection should execute");

        assert_eq!(result.get_i32(0), Some(5));
        assert_eq!(result.get_i32(1), Some(99));
        assert_eq!(result.get_i32(2), Some(7));
    }

    #[test]
    fn in_list_uses_small_and_hashed_constant_strategies_with_sql_null_semantics() {
        let session = test_session();
        let runtime = test_runtime(session.clone());
        let small_expr = in_list_i32_expr(&[Some(2), Some(4), None], false);
        let large_values = (0..16).map(Some).collect::<Vec<_>>();
        let large_expr = in_list_i32_expr(&large_values, true);
        let mut small_executor = ExpressionExecutor::new(&small_expr);
        let mut large_executor = ExpressionExecutor::new(&large_expr);
        let input = nullable_integer_chunk(&[Some(2), Some(3), None, Some(20)]);
        let mut small_result =
            paro_common::test_utils::test_vector_with_capacity(LogicalType::Boolean, input.size());
        let mut large_result =
            paro_common::test_utils::test_vector_with_capacity(LogicalType::Boolean, input.size());

        small_executor
            .execute_into(0, &input, None, input.size(), &runtime, &mut small_result)
            .expect("small IN should execute");
        large_executor
            .execute_into(0, &input, None, input.size(), &runtime, &mut large_result)
            .expect("large NOT IN should execute");

        assert_eq!(small_result.get_bool(0), Some(true));
        assert_eq!(small_result.get_bool(1), None);
        assert_eq!(small_result.get_bool(2), None);
        assert_eq!(small_result.get_bool(3), None);

        assert_eq!(large_result.get_bool(0), Some(false));
        assert_eq!(large_result.get_bool(1), Some(false));
        assert_eq!(large_result.get_bool(2), None);
        assert_eq!(large_result.get_bool(3), Some(true));

        match small_executor.compiled_state(0) {
            CompiledExpressionState::Operator(state) => {
                assert!(matches!(
                    state.in_list.as_ref(),
                    Some(PreparedInList::SmallConst { .. })
                ));
            }
            other => panic!("expected operator state, got {other:?}"),
        }
        match large_executor.compiled_state(0) {
            CompiledExpressionState::Operator(state) => {
                assert!(matches!(
                    state.in_list.as_ref(),
                    Some(PreparedInList::HashedConst { .. })
                ));
            }
            other => panic!("expected operator state, got {other:?}"),
        }
    }

    #[test]
    fn not_operator_respects_selection_overlay_from_filter_results() {
        let session = test_session();
        let runtime = test_runtime(session);
        let expr = Expression::Operator(OperatorExpression::new(
            OperatorType::Not,
            vec![reference_bool(0)],
            LogicalType::Boolean,
        ));
        let mut executor = ExpressionExecutor::new(&expr);
        let input = boolean_chunk(&[Some(true), None, Some(false)]);
        let selection = paro_common::test_utils::test_selection(vec![2, 0, 1]);
        let mut result = paro_common::test_utils::test_vector_with_capacity(
            LogicalType::Boolean,
            selection.len(),
        );

        executor
            .execute_into(
                0,
                &input,
                Some(&selection),
                selection.len(),
                &runtime,
                &mut result,
            )
            .expect("NOT should execute over selection overlays");

        assert_eq!(result.get_bool(0), Some(true));
        assert_eq!(result.get_bool(1), Some(false));
        assert!(result.is_null(2));
    }

    #[test]
    fn storage_dictionary_cache_reuses_unique_result_for_same_provenance() {
        let session = test_session();
        let runtime = test_runtime(session);
        let (expr, counter) = cached_identity_expr(0);
        let mut executor = ExpressionExecutor::new(&expr);

        let first_input = Chunk::from_vectors(
            vec![storage_dictionary_i32(&[10, 20, 30], vec![2, 0, 1, 0], 41)],
            paro_common::test_utils::test_allocator(),
        );
        let second_input = Chunk::from_vectors(
            vec![storage_dictionary_i32(&[10, 20, 30], vec![1, 1, 2], 41)],
            paro_common::test_utils::test_allocator(),
        );

        let first = executor
            .execute_expression(0, &first_input, None, first_input.size(), &runtime)
            .expect("first cached dictionary execution should succeed");
        let second = executor
            .execute_expression(0, &second_input, None, second_input.size(), &runtime)
            .expect("second cached dictionary execution should succeed");

        assert_eq!(counter.load(Ordering::SeqCst), 1);
        assert_eq!(first.get_i32(0), Some(30));
        assert_eq!(first.get_i32(1), Some(10));
        assert_eq!(first.get_i32(2), Some(20));
        assert_eq!(second.get_i32(0), Some(20));
        assert_eq!(second.get_i32(1), Some(20));
        assert_eq!(second.get_i32(2), Some(30));
    }

    #[test]
    fn generic_selection_dictionary_never_uses_storage_dictionary_cache() {
        let session = test_session();
        let runtime = test_runtime(session);
        let (expr, counter) = cached_identity_expr(0);
        let mut executor = ExpressionExecutor::new(&expr);
        let input = Chunk::from_vectors(
            vec![paro_common::test_utils::test_dictionary(
                Arc::new(paro_common::test_utils::test_i32_vector_with_allocator(
                    &[10, 20, 30],
                    paro_common::test_utils::test_allocator(),
                )),
                vec![2, 0, 1, 0],
            )],
            paro_common::test_utils::test_allocator(),
        );

        let _ = executor
            .execute_expression(0, &input, None, input.size(), &runtime)
            .expect("generic dictionary first execution should succeed");
        let _ = executor
            .execute_expression(0, &input, None, input.size(), &runtime)
            .expect("generic dictionary second execution should succeed");

        assert_eq!(counter.load(Ordering::SeqCst), 2);
        match executor.compiled_state(0) {
            CompiledExpressionState::Function(state) => {
                assert!(state.cached_dictionary_output.is_none());
            }
            other => panic!("expected function state, got {other:?}"),
        }
    }

    #[test]
    fn storage_dictionary_cache_requires_non_driving_arguments_to_stay_constant() {
        let session = test_session();
        let runtime = test_runtime(session);
        let (expr, counter) = cached_add_pair_expr(0, 1);
        let mut executor = ExpressionExecutor::new(&expr);
        let input = Chunk::from_vectors(
            vec![
                storage_dictionary_i32(&[10, 20, 30], vec![2, 0, 1, 0], 52),
                paro_common::test_utils::test_i32_vector_with_allocator(
                    &[1, 1, 1, 1],
                    paro_common::test_utils::test_allocator(),
                ),
            ],
            paro_common::test_utils::test_allocator(),
        );

        let first = executor
            .execute_expression(0, &input, None, input.size(), &runtime)
            .expect("non-constant companion first execution should succeed");
        let second = executor
            .execute_expression(0, &input, None, input.size(), &runtime)
            .expect("non-constant companion second execution should succeed");

        assert_eq!(counter.load(Ordering::SeqCst), 2);
        assert_eq!(first.get_i32(0), Some(31));
        assert_eq!(second.get_i32(3), Some(11));
    }

    #[test]
    fn storage_dictionary_cache_misses_when_provenance_changes() {
        let session = test_session();
        let runtime = test_runtime(session);
        let (expr, counter) = cached_identity_expr(0);
        let mut executor = ExpressionExecutor::new(&expr);

        let first_input = Chunk::from_vectors(
            vec![storage_dictionary_i32(&[10, 20, 30], vec![2, 0, 1], 100)],
            paro_common::test_utils::test_allocator(),
        );
        let second_input = Chunk::from_vectors(
            vec![storage_dictionary_i32(&[10, 20, 30], vec![2, 0, 1], 101)],
            paro_common::test_utils::test_allocator(),
        );

        let _ = executor
            .execute_expression(0, &first_input, None, first_input.size(), &runtime)
            .expect("first provenance execution should succeed");
        let _ = executor
            .execute_expression(0, &second_input, None, second_input.size(), &runtime)
            .expect("second provenance execution should succeed");

        assert_eq!(counter.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn comparison_handles_dictionary_inputs_from_join_results() {
        let session = test_session();
        let runtime = test_runtime(session);
        let expr = Expression::Comparison(ComparisonExpression::new(
            ComparisonType::Equal,
            reference_i32(0),
            reference_i32(1),
        ));
        let mut executor = ExpressionExecutor::new(&expr);
        let left = paro_common::test_utils::test_dictionary(
            Arc::new(paro_common::test_utils::test_i32_vector_with_allocator(
                &[10, 20, 30],
                paro_common::test_utils::test_allocator(),
            )),
            vec![2, 0, 1],
        );
        let right = paro_common::test_utils::test_dictionary(
            Arc::new(paro_common::test_utils::test_i32_vector_with_allocator(
                &[30, 10, 10],
                paro_common::test_utils::test_allocator(),
            )),
            vec![0, 1, 1],
        );
        let input = paro_common::test_utils::test_chunk_from_vectors(vec![left, right]);

        let result = executor
            .execute_expression(0, &input, None, input.size(), &runtime)
            .expect("comparison should execute over dictionary inputs");

        assert_eq!(result.get_bool(0), Some(true));
        assert_eq!(result.get_bool(1), Some(true));
        assert_eq!(result.get_bool(2), Some(false));
    }

    #[test]
    fn fixed_width_cast_handles_dictionary_inputs_from_join_results() {
        let session = test_session();
        let runtime = test_runtime(session);
        let expr = cast_expr(
            reference_i32(0),
            LogicalType::BigInt,
            BoundCastInfo::fixed(numeric_casts::int32_to_int64),
            false,
        );
        let mut executor = ExpressionExecutor::new(&expr);
        let input = Chunk::from_vectors(
            vec![paro_common::test_utils::test_dictionary(
                Arc::new(paro_common::test_utils::test_i32_vector_with_allocator(
                    &[10, 20, 30],
                    paro_common::test_utils::test_allocator(),
                )),
                vec![2, 0, 1],
            )],
            paro_common::test_utils::test_allocator(),
        );

        let result = executor
            .execute_expression(0, &input, None, input.size(), &runtime)
            .expect("fixed-width cast should execute over dictionary inputs");

        assert_eq!(result.get_i64(0), Some(30));
        assert_eq!(result.get_i64(1), Some(10));
        assert_eq!(result.get_i64(2), Some(20));
    }

    #[test]
    fn try_cast_nullifies_out_of_range_dictionary_values() {
        let session = test_session();
        let runtime = test_runtime(session);
        let expr = cast_expr(
            reference_i64(0),
            LogicalType::TinyInt,
            BoundCastInfo::fixed(numeric_casts::int64_to_int8),
            true,
        );
        let mut executor = ExpressionExecutor::new(&expr);
        let input = Chunk::from_vectors(
            vec![paro_common::test_utils::test_dictionary(
                Arc::new(paro_common::test_utils::test_i64_vector_with_allocator(
                    &[127, 128, -129],
                    paro_common::test_utils::test_allocator(),
                )),
                vec![2, 0, 1],
            )],
            paro_common::test_utils::test_allocator(),
        );

        let result = executor
            .execute_expression(0, &input, None, input.size(), &runtime)
            .expect("TRY_CAST should execute over dictionary inputs");

        assert!(result.is_null(0));
        assert_eq!(result.get_i8(1), Some(127));
        assert!(result.is_null(2));
    }

    #[test]
    fn fixed_width_cast_reads_sequence_without_materializing() {
        let session = test_session();
        let runtime = test_runtime(session);
        let expr = cast_expr(
            reference_i64(0),
            LogicalType::Double,
            BoundCastInfo::fixed(numeric_casts::int64_to_double),
            false,
        );
        let mut executor = ExpressionExecutor::new(&expr);
        let input = Chunk::from_vectors(
            vec![paro_common::test_utils::test_sequence_with_allocator(
                10,
                3,
                4,
                paro_common::test_utils::test_allocator(),
            )],
            paro_common::test_utils::test_allocator(),
        );

        let result = executor
            .execute_expression(0, &input, None, input.size(), &runtime)
            .expect("sequence cast should execute");

        assert_eq!(result.get_f64(0), Some(10.0));
        assert_eq!(result.get_f64(1), Some(13.0));
        assert_eq!(result.get_f64(2), Some(16.0));
        assert_eq!(result.get_f64(3), Some(19.0));
    }

    #[test]
    fn timestamp_cast_preserves_values_on_fixed_identity_path() {
        let session = test_session();
        let runtime = test_runtime(session);
        let expr = cast_expr(
            reference_timestamp(0),
            LogicalType::TimestampTz,
            BoundCastInfo::identity(&LogicalType::Timestamp, &LogicalType::TimestampTz),
            false,
        );
        let mut executor = ExpressionExecutor::new(&expr);
        let input = Chunk::from_vectors(
            vec![vector_from_i64_values(
                LogicalType::Timestamp,
                &[1_000_000, -42],
            )],
            paro_common::test_utils::test_allocator(),
        );

        let result = executor
            .execute_expression(0, &input, None, input.size(), &runtime)
            .expect("timestamp cast should execute");

        assert_eq!(result.logical_type(), &LogicalType::TimestampTz);
        assert_eq!(result.get_i64(0), Some(1_000_000));
        assert_eq!(result.get_i64(1), Some(-42));
    }

    #[test]
    fn reference_expression_uses_dictionary_overlay_when_selection_is_present() {
        let session = test_session();
        let runtime = test_runtime(session);
        let expr = reference_i32(0);
        let mut executor = ExpressionExecutor::new(&expr);
        let input = integer_chunk(&[10, 20, 30]);
        let selection = paro_common::test_utils::test_selection(vec![2, 0]);
        let selection_allocation = selection.allocation_identity();

        let selected = executor
            .execute_expression(0, &input, Some(&selection), selection.len(), &runtime)
            .expect("reference expression with selection should execute");
        assert_eq!(selected.vector_type(), VectorType::Dictionary);
        assert!(Arc::ptr_eq(
            selected.child().expect("dictionary child should exist"),
            input.column(0).expect("input column should exist")
        ));
        assert_eq!(
            selected
                .sel_vector()
                .expect("dictionary selection should exist")
                .allocation_identity(),
            selection_allocation
        );
        assert_eq!(selected.get_i32(0), Some(30));
        assert_eq!(selected.get_i32(1), Some(10));

        let flat = executor
            .execute_expression(0, &input, None, input.size(), &runtime)
            .expect("reference expression without selection should execute");
        assert_eq!(flat.vector_type(), VectorType::Flat);
    }

    #[test]
    fn column_ref_expression_uses_dictionary_overlay_when_selection_is_present() {
        let session = test_session();
        let runtime = test_runtime(session);
        let expr = Expression::ColumnRef(ColumnRefExpression::new(
            ColumnBinding::new(0, 0),
            LogicalType::Integer,
        ));
        let mut executor = ExpressionExecutor::new(&expr);
        let input = integer_chunk(&[10, 20, 30]);
        let selection = paro_common::test_utils::test_selection(vec![1, 2]);
        let selection_allocation = selection.allocation_identity();

        let selected = executor
            .execute_expression(0, &input, Some(&selection), selection.len(), &runtime)
            .expect("column ref with selection should execute");
        assert_eq!(selected.vector_type(), VectorType::Dictionary);
        assert!(Arc::ptr_eq(
            selected.child().expect("dictionary child should exist"),
            input.column(0).expect("input column should exist")
        ));
        assert_eq!(
            selected
                .sel_vector()
                .expect("dictionary selection should exist")
                .allocation_identity(),
            selection_allocation
        );
        assert_eq!(selected.get_i32(0), Some(20));
        assert_eq!(selected.get_i32(1), Some(30));
    }

    #[test]
    fn compiled_program_owns_expression_storage() {
        let session = test_session();
        let runtime = test_runtime(session);
        let mut executor = {
            let expr = add_one_expr(0);
            ExpressionExecutor::new(&expr)
        };
        let input = integer_chunk(&[1, 4]);

        let result = executor
            .execute_expression(0, &input, None, input.size(), &runtime)
            .expect("compiled executor should not borrow the original expression");

        assert_eq!(result.get_i32(0), Some(2));
        assert_eq!(result.get_i32(1), Some(5));
    }

    #[test]
    fn function_intermediate_chunk_allocates_lazily_and_reuses_capacity() {
        let session = test_session();
        let runtime = test_runtime(session);
        let expr = add_one_expr(0);
        let mut executor = ExpressionExecutor::new(&expr);

        let function_state = match &executor.state.states[0] {
            CompiledExpressionState::Function(state) => state,
            other => panic!("expected function state, got {other:?}"),
        };
        assert!(function_state.intermediate_chunk.is_none());

        let mut result =
            paro_common::test_utils::test_vector_with_capacity(LogicalType::Integer, 1);
        let first_input = integer_chunk(&[1, 2]);
        executor
            .execute_into(
                0,
                &first_input,
                None,
                first_input.size(),
                &runtime,
                &mut result,
            )
            .expect("first execute_into should succeed");
        assert_eq!(result.get_i32(0), Some(2));
        assert_eq!(result.get_i32(1), Some(3));

        let function_state = match &executor.state.states[0] {
            CompiledExpressionState::Function(state) => state,
            _ => unreachable!(),
        };
        let first_chunk = function_state
            .intermediate_chunk
            .as_ref()
            .expect("intermediate chunk should be initialized after first execution");
        let first_capacity = first_chunk.capacity();
        assert_eq!(first_chunk.column_count(), 1);

        let smaller_input = integer_chunk(&[7]);
        executor
            .execute_into(
                0,
                &smaller_input,
                None,
                smaller_input.size(),
                &runtime,
                &mut result,
            )
            .expect("second execute_into should succeed");

        let function_state = match &executor.state.states[0] {
            CompiledExpressionState::Function(state) => state,
            _ => unreachable!(),
        };
        let reused_chunk = function_state
            .intermediate_chunk
            .as_ref()
            .expect("intermediate chunk should stay allocated");
        assert_eq!(reused_chunk.capacity(), first_capacity);
        assert_eq!(reused_chunk.column_count(), 1);

        let larger_input = integer_chunk(&[1, 2, 3, 4, 5]);
        executor
            .execute_into(
                0,
                &larger_input,
                None,
                larger_input.size(),
                &runtime,
                &mut result,
            )
            .expect("third execute_into should succeed");
        let function_state = match &executor.state.states[0] {
            CompiledExpressionState::Function(state) => state,
            _ => unreachable!(),
        };
        let expanded_chunk = function_state
            .intermediate_chunk
            .as_ref()
            .expect("intermediate chunk should remain allocated");
        assert!(expanded_chunk.capacity() >= larger_input.size());
        assert_eq!(expanded_chunk.column_count(), 1);
    }

    #[test]
    fn function_local_state_is_initialized_once_per_executor() {
        LOCAL_STATE_INIT_COUNT.store(0, Ordering::SeqCst);

        let session = test_session();
        let runtime = test_runtime(session);
        let expr = add_with_local_state_expr(0);
        let input = integer_chunk(&[1, 2, 3]);
        let mut executor = ExpressionExecutor::new(&expr);

        let first = executor
            .execute_expression(0, &input, None, input.size(), &runtime)
            .expect("first execution");
        let second = executor
            .execute_expression(0, &input, None, input.size(), &runtime)
            .expect("second execution");

        assert_eq!(LOCAL_STATE_INIT_COUNT.load(Ordering::SeqCst), 1);
        assert_eq!(first.get_i32(0), Some(8));
        assert_eq!(first.get_i32(2), Some(10));
        assert_eq!(second.get_i32(1), Some(9));
    }

    #[test]
    fn execute_all_into_reuses_output_chunk_columns() {
        let session = test_session();
        let runtime = test_runtime(session);
        let mut executor = ExpressionExecutor::with_expressions(&[add_one_expr(0)]);
        let mut output = Chunk::try_new(paro_common::test_utils::test_allocator())
            .expect("test chunk allocation failed");

        let first_input = integer_chunk(&[1, 2, 3]);
        executor
            .execute_all_into(&first_input, &runtime, &mut output)
            .expect("first execute_all_into should succeed");
        let first_output_ptr =
            Arc::as_ptr(output.column(0).expect("output column should exist")) as usize;
        assert_eq!(output.get_value(0, 0), Some(Value::Integer(2)));

        let second_input = integer_chunk(&[4, 5]);
        executor
            .execute_all_into(&second_input, &runtime, &mut output)
            .expect("second execute_all_into should succeed");
        let second_output_ptr =
            Arc::as_ptr(output.column(0).expect("output column should exist")) as usize;
        assert_eq!(first_output_ptr, second_output_ptr);
        assert_eq!(output.get_value(0, 0), Some(Value::Integer(5)));
        assert_eq!(output.get_value(0, 1), Some(Value::Integer(6)));
    }

    #[test]
    fn conjunction_uses_ping_pong_scratch_slots() {
        let session = test_session();
        let runtime = test_runtime(session);
        let expr = Expression::Conjunction(ConjunctionExpression::new(
            ConjunctionType::And,
            vec![
                greater_than_i32(0, 0),
                less_than_i32(0, 10),
                greater_than_i32(0, 3),
            ],
        ));
        let mut executor = ExpressionExecutor::new(&expr);
        let input = integer_chunk(&[5, 8]);

        let result = executor
            .execute_expression(0, &input, None, input.size(), &runtime)
            .expect("conjunction should execute");
        assert_eq!(result.get_bool(0), Some(true));
        assert_eq!(result.get_bool(1), Some(true));

        let state = match &executor.state.states[0] {
            CompiledExpressionState::Conjunction(state) => state,
            other => panic!("expected conjunction state, got {other:?}"),
        };
        let ping_capacity = state
            .ping
            .as_ref()
            .expect("ping scratch should be initialized")
            .capacity();
        let pong_capacity = state
            .pong
            .as_ref()
            .expect("pong scratch should be initialized")
            .capacity();

        let smaller_input = integer_chunk(&[6]);
        executor
            .execute_expression(0, &smaller_input, None, smaller_input.size(), &runtime)
            .expect("second conjunction execution should succeed");
        let state = match &executor.state.states[0] {
            CompiledExpressionState::Conjunction(state) => state,
            _ => unreachable!(),
        };
        assert_eq!(
            state
                .ping
                .as_ref()
                .expect("ping scratch should stay allocated")
                .capacity(),
            ping_capacity
        );
        assert_eq!(
            state
                .pong
                .as_ref()
                .expect("pong scratch should stay allocated")
                .capacity(),
            pong_capacity
        );
    }
}
