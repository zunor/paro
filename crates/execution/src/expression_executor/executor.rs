// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Compiled expression executor.

mod fusion;

use std::cell::{Cell, RefCell};
use std::collections::HashSet;
use std::fmt;
use std::sync::Arc;

use smallvec::SmallVec;

use paro_common::allocator::{Allocator, MemoryTag};
use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::runtime_value::Value;
use paro_common::typed_parameters::ParameterSlot;
use paro_common::types::LogicalType;
use paro_common::vector::{DictionarySource, SelectionVector, Vector, VectorType};
use paro_function::scalar::cast::CastExecCtx;
use paro_function::scalar::executor::variadic::apply_coalesce_child;
use paro_function::scalar::operators::logic::LogicExecutor;
use paro_function::scalar::{
    DictionaryStrategy, FunctionErrorMode, FunctionExecContext, FunctionSideEffects,
    FunctionStability,
};
use paro_planner::expression::{Expression, OperatorType};

use crate::runtime::{ExpressionEvalInput, ParameterBindings};

use super::comparison::{compile_comparison_dispatch, COMPARISON_EXEC_CTX};
use super::execution_state::BoundFunctionContext;
use super::like_pattern::{select_prepared_like, sql_like, PreparedLikePattern};
use super::predicate::{
    accumulate_selected_rows, build_marked_selection, copy_selection, scan_bool_selection,
    scan_false_bool_selection, scan_null_selection, select_all_rows,
};
use super::program::{
    ExpressionProgramCache, ExpressionProgramVersion, PhysicalCaseExpression,
    PhysicalCastExpression, PhysicalColumnRefExpression, PhysicalComparisonExpression,
    PhysicalConjunctionExpression, PhysicalExpression, PhysicalExpressionProgram,
    PhysicalFunctionExpression, PhysicalOperatorExpression, PhysicalReferenceExpression,
    PhysicalSharedExpression,
};
use super::state::{
    CachedDictionaryInputId, CaseExpressionState, CastExpressionState, ColumnRefExpressionState,
    ComparisonExpressionState, CompiledExpressionState, ConjunctionExpressionState,
    ConstantExpressionState, EvaluatedValue, ExecuteFunctionState, OperatorExpressionState,
    ParameterExpressionState, PreparedInList, ReferenceExpressionState, SharedBatchSignature,
    SharedExpressionSlot, SharedExpressionState, ValueSlot,
};

#[derive(Debug)]
pub struct CompiledExpressionProgram {
    physical: Arc<PhysicalExpressionProgram>,
}

#[derive(Debug)]
pub struct CompiledExecutorState {
    states: Vec<CompiledExpressionState>,
    shared_states: Vec<SharedStateSlot>,
    shared_slots: Vec<SharedExpressionSlot>,
    batch_epoch: u64,
}

impl CompiledExecutorState {
    fn release_batch_references(&mut self) {
        for state in &mut self.states {
            state.release_batch_references();
        }
        for state in &self.shared_states {
            if let Some(mut state) = SharedStateLease::take(state) {
                state.state_mut().release_batch_references();
            }
        }
        for slot in &mut self.shared_slots {
            slot.value = ValueSlot::Empty;
            slot.signature = None;
        }
    }
}

#[derive(Debug)]
pub struct ExpressionExecutor {
    pub program: CompiledExpressionProgram,
    pub state: CompiledExecutorState,
}

#[derive(Clone, Copy)]
pub struct VectorKernelInput<'a> {
    pub columns: &'a Chunk,
    pub params: Option<&'a ParameterBindings>,
    pub selection: Option<&'a SelectionVector>,
    pub count: usize,
}

struct FusedOutputSet {
    outputs: SmallVec<[bool; 64]>,
}

impl FusedOutputSet {
    fn new(output_count: usize) -> Self {
        let mut outputs = SmallVec::new();
        outputs.resize(output_count, false);
        Self { outputs }
    }

    fn pair_is_available(&self, first: usize, second: usize) -> bool {
        !self.outputs[first] && !self.outputs[second]
    }

    fn mark_pair(&mut self, first: usize, second: usize) {
        self.outputs[first] = true;
        self.outputs[second] = true;
    }

    fn contains(&self, output: usize) -> bool {
        self.outputs[output]
    }
}

impl<'a> VectorKernelInput<'a> {
    pub fn from_chunk(columns: &'a Chunk) -> Self {
        Self {
            columns,
            params: None,
            selection: None,
            count: columns.size(),
        }
    }

    pub fn from_eval_input(input: ExpressionEvalInput<'a>) -> Self {
        Self {
            columns: input.columns,
            params: Some(input.params),
            selection: None,
            count: input.columns.size(),
        }
    }

    pub fn with_selection(mut self, selection: Option<&'a SelectionVector>) -> Self {
        self.selection = selection;
        self
    }

    pub fn with_count(mut self, count: usize) -> Self {
        self.count = count;
        self
    }
}

struct SharedEvaluation<'a> {
    nodes: &'a [PhysicalExpression],
    states: &'a [SharedStateSlot],
    slots: &'a mut [SharedExpressionSlot],
    epoch: u64,
}

/// Temporarily owns a shared expression state and restores it on every exit,
/// including panic unwinding. State cells and scratch slots are disjoint
/// fields, so nested evaluation can reborrow the latter without a lock, raw
/// pointer, or unwind fence in the vector hot path.
struct SharedStateLease<'a> {
    slot: &'a SharedStateSlot,
    state: Option<CompiledExpressionState>,
}

struct SharedStateSlot(Cell<Option<CompiledExpressionState>>);

impl SharedStateSlot {
    fn new(state: CompiledExpressionState) -> Self {
        Self(Cell::new(Some(state)))
    }
}

impl fmt::Debug for SharedStateSlot {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("SharedStateSlot(..)")
    }
}

impl<'a> SharedStateLease<'a> {
    fn take(slot: &'a SharedStateSlot) -> Option<Self> {
        let state = slot.0.take()?;
        Some(Self {
            slot,
            state: Some(state),
        })
    }

    fn state_mut(&mut self) -> &mut CompiledExpressionState {
        self.state
            .as_mut()
            .expect("shared state lease always owns its state")
    }
}

impl Drop for SharedStateLease<'_> {
    fn drop(&mut self) {
        let previous = self.slot.0.replace(Some(
            self.state
                .take()
                .expect("shared state lease always restores its state"),
        ));
        debug_assert!(previous.is_none(), "shared state slot restored twice");
    }
}

thread_local! {
    static THREAD_LOCAL_PROGRAM_CACHE: RefCell<ExpressionProgramCache> =
        RefCell::new(ExpressionProgramCache::default());
}

impl SharedEvaluation<'_> {
    fn signature(&self, sel: Option<&SelectionVector>, count: usize) -> SharedBatchSignature {
        SharedBatchSignature {
            epoch: self.epoch,
            count,
            selection_identity: selection_identity(sel),
            selection_hash: selection_hash(sel, count),
        }
    }

    fn execute_value(
        &mut self,
        expr: &PhysicalSharedExpression,
        chunk: &Chunk,
        sel: Option<&SelectionVector>,
        count: usize,
        runtime: &dyn FunctionExecContext,
        params: Option<&ParameterBindings>,
    ) -> Result<EvaluatedValue> {
        let node = self
            .nodes
            .get(expr.slot)
            .ok_or_else(|| paro_error::internal("shared expression node out of bounds"))?;

        let signature = self.signature(sel, count);
        if self
            .slots
            .get(expr.slot)
            .is_some_and(|slot| slot.signature == Some(signature))
        {
            return self.slots[expr.slot]
                .value
                .evaluated(true)
                .ok_or_else(|| paro_error::internal("shared expression cache slot was empty"));
        }

        let value = self.execute_uncached(expr.slot, node, chunk, sel, count, runtime, params)?;
        let slot = self
            .slots
            .get_mut(expr.slot)
            .ok_or_else(|| paro_error::internal("shared expression scratch slot out of bounds"))?;
        slot.value.set_value(value.as_vector().reference());
        slot.signature = Some(signature);
        slot.value
            .evaluated(true)
            .ok_or_else(|| paro_error::internal("shared expression cache slot was not stored"))
    }

    /// Execute a shared expression root directly into its caller-owned output.
    ///
    /// Shared child evaluation keeps node-local scratch because several
    /// parents may borrow it. A root already has a stable batch-lifetime owner:
    /// the output chunk. Caching a reference to that owner avoids copying the
    /// complete scratch vector into the output and lets the output allocation
    /// be reused after the cache is cleared at the next batch boundary.
    fn execute_into(
        &mut self,
        expr: &PhysicalSharedExpression,
        chunk: &Chunk,
        sel: Option<&SelectionVector>,
        count: usize,
        runtime: &dyn FunctionExecContext,
        params: Option<&ParameterBindings>,
        result: &mut Vector,
    ) -> Result<()> {
        let signature = self.signature(sel, count);
        if self
            .slots
            .get(expr.slot)
            .is_some_and(|slot| slot.signature == Some(signature))
        {
            return self.slots[expr.slot]
                .value
                .evaluated(false)
                .ok_or_else(|| paro_error::internal("shared expression cache slot was empty"))?
                .write_into(result);
        }

        let nodes = self.nodes;
        let states = self.states;
        let epoch = self.epoch;
        let slots = &mut *self.slots;
        let node = nodes
            .get(expr.slot)
            .ok_or_else(|| paro_error::internal("shared expression node out of bounds"))?;
        let state_slot = states
            .get(expr.slot)
            .ok_or_else(|| paro_error::internal("shared expression state out of bounds"))?;
        let mut state = SharedStateLease::take(state_slot)
            .ok_or_else(|| paro_error::internal("recursive shared expression evaluation"))?;
        let mut nested = SharedEvaluation {
            nodes,
            states,
            slots,
            epoch,
        };
        ExpressionExecutor::execute_into_inner(
            node,
            state.state_mut(),
            chunk,
            sel,
            count,
            runtime,
            params,
            result,
            &mut nested,
        )?;
        drop(state);

        let slot = nested
            .slots
            .get_mut(expr.slot)
            .ok_or_else(|| paro_error::internal("shared expression scratch slot out of bounds"))?;
        slot.value.set_value(result.reference());
        slot.signature = Some(signature);
        Ok(())
    }

    fn execute_uncached(
        &mut self,
        slot: usize,
        node: &PhysicalExpression,
        chunk: &Chunk,
        sel: Option<&SelectionVector>,
        count: usize,
        runtime: &dyn FunctionExecContext,
        params: Option<&ParameterBindings>,
    ) -> Result<EvaluatedValue> {
        let nodes = self.nodes;
        let states = self.states;
        let epoch = self.epoch;
        let slots = &mut *self.slots;
        let state_slot = states
            .get(slot)
            .ok_or_else(|| paro_error::internal("shared expression state out of bounds"))?;
        let mut state = SharedStateLease::take(state_slot)
            .ok_or_else(|| paro_error::internal("recursive shared expression evaluation"))?;
        let mut nested = SharedEvaluation {
            nodes,
            states,
            slots,
            epoch,
        };
        ExpressionExecutor::execute_value(
            node,
            state.state_mut(),
            chunk,
            sel,
            count,
            runtime,
            params,
            &mut nested,
        )
    }
}

fn selection_identity(sel: Option<&SelectionVector>) -> usize {
    sel.and_then(SelectionVector::allocation_identity)
        .unwrap_or_default()
}

fn selection_hash(sel: Option<&SelectionVector>, count: usize) -> u64 {
    let Some(sel) = sel else {
        return 0;
    };
    let mut hash = 0xcbf29ce484222325u64;
    hash ^= count as u64;
    hash = hash.wrapping_mul(0x100000001b3);
    for row in &sel.as_slice()[..count] {
        hash ^= u64::from(*row);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

fn expression_state_mismatch(
    expr: &PhysicalExpression,
    state: &CompiledExpressionState,
) -> paro_error::ParoError {
    paro_error::internal(format!(
        "Expression state mismatch: expr={} state={}",
        physical_expression_kind(expr),
        compiled_expression_state_kind(state)
    ))
}

fn physical_expression_kind(expr: &PhysicalExpression) -> &'static str {
    match expr {
        PhysicalExpression::Function(_) => "Function",
        PhysicalExpression::Cast(_) => "Cast",
        PhysicalExpression::Comparison(_) => "Comparison",
        PhysicalExpression::Conjunction(_) => "Conjunction",
        PhysicalExpression::Case(_) => "Case",
        PhysicalExpression::Operator(_) => "Operator",
        PhysicalExpression::Constant(_) => "Constant",
        PhysicalExpression::Parameter(_) => "Parameter",
        PhysicalExpression::ColumnRef(_) => "ColumnRef",
        PhysicalExpression::Reference(_) => "Reference",
        PhysicalExpression::Shared(_) => "Shared",
    }
}

fn compiled_expression_state_kind(state: &CompiledExpressionState) -> &'static str {
    match state {
        CompiledExpressionState::Function(_) => "Function",
        CompiledExpressionState::Cast(_) => "Cast",
        CompiledExpressionState::Comparison(_) => "Comparison",
        CompiledExpressionState::Conjunction(_) => "Conjunction",
        CompiledExpressionState::Case(_) => "Case",
        CompiledExpressionState::Operator(_) => "Operator",
        CompiledExpressionState::Constant(_) => "Constant",
        CompiledExpressionState::Parameter(_) => "Parameter",
        CompiledExpressionState::ColumnRef(_) => "ColumnRef",
        CompiledExpressionState::Reference(_) => "Reference",
        CompiledExpressionState::Shared(_) => "Shared",
        CompiledExpressionState::Subquery(_) => "Subquery",
    }
}

impl ExpressionExecutor {
    pub fn new(expr: &Expression) -> Self {
        Self::with_expressions(std::slice::from_ref(expr))
    }

    pub fn with_expressions(exprs: &[Expression]) -> Self {
        Self::with_expressions_and_version(exprs, ExpressionProgramVersion::anonymous())
    }

    pub fn with_expressions_for_session(
        exprs: &[Expression],
        session: &paro_context::StatementContext,
    ) -> Self {
        Self::with_expressions_and_version(exprs, ExpressionProgramVersion::from_session(session))
    }

    pub fn with_expressions_and_version(
        exprs: &[Expression],
        version: ExpressionProgramVersion,
    ) -> Self {
        let physical = Self::cached_program(exprs, version);
        Self::from_physical(physical)
    }

    pub(crate) fn with_expression_refs_for_session(
        exprs: &[&Expression],
        session: &paro_context::StatementContext,
    ) -> Self {
        let physical =
            Self::cached_program_refs(exprs, ExpressionProgramVersion::from_session(session));
        Self::from_physical(physical)
    }

    fn from_physical(physical: Arc<PhysicalExpressionProgram>) -> Self {
        let states = (0..physical.unique_root_count())
            .map(|root_idx| Self::initialize(physical.unique_root(root_idx)))
            .collect();
        let shared_states = (0..physical.shared_expression_count())
            .map(|slot| SharedStateSlot::new(Self::initialize(physical.shared_node(slot))))
            .collect();
        let shared_slots = physical
            .scratch_layout()
            .slots()
            .iter()
            .map(|_| SharedExpressionSlot::default())
            .collect();
        Self {
            program: CompiledExpressionProgram { physical },
            state: CompiledExecutorState {
                states,
                shared_states,
                shared_slots,
                batch_epoch: 0,
            },
        }
    }

    fn cached_program(
        exprs: &[Expression],
        version: ExpressionProgramVersion,
    ) -> Arc<PhysicalExpressionProgram> {
        THREAD_LOCAL_PROGRAM_CACHE.with(|cache| {
            let mut cache = cache.borrow_mut();
            cache.get_or_compile(exprs, version)
        })
    }

    fn cached_program_refs(
        exprs: &[&Expression],
        version: ExpressionProgramVersion,
    ) -> Arc<PhysicalExpressionProgram> {
        THREAD_LOCAL_PROGRAM_CACHE.with(|cache| {
            let mut cache = cache.borrow_mut();
            cache.get_or_compile_refs(exprs, version)
        })
    }

    pub fn expression_count(&self) -> usize {
        self.program.physical.root_count()
    }

    pub fn physical_program(&self) -> &PhysicalExpressionProgram {
        &self.program.physical
    }

    #[cfg(test)]
    pub(crate) fn compiled_state(&self, expr_idx: usize) -> &CompiledExpressionState {
        &self.state.states[self.program.physical.root_state_index(expr_idx)]
    }

    pub fn initialize(expr: &PhysicalExpression) -> CompiledExpressionState {
        match expr {
            PhysicalExpression::Function(e) => {
                CompiledExpressionState::Function(ExecuteFunctionState {
                    child_states: e.children.iter().map(Self::initialize).collect(),
                    intermediate_types: e
                        .children
                        .iter()
                        .map(PhysicalExpression::return_type)
                        .collect(),
                    intermediate_chunk: None,
                    local_state: None,
                    cached_dictionary_input_id: None,
                    cached_dictionary_output: None,
                    result: ValueSlot::default(),
                })
            }
            PhysicalExpression::Cast(e) => CompiledExpressionState::Cast(CastExpressionState {
                child: Box::new(Self::initialize(&e.child)),
                child_result: ValueSlot::default(),
                result: ValueSlot::default(),
            }),
            PhysicalExpression::Comparison(e) => {
                let dispatch = compile_comparison_dispatch(&e.left_type, e.comparison_type);
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
            PhysicalExpression::Conjunction(e) => {
                CompiledExpressionState::Conjunction(ConjunctionExpressionState {
                    child_states: e.children.iter().map(Self::initialize).collect(),
                    ping: ValueSlot::default(),
                    pong: ValueSlot::default(),
                })
            }
            PhysicalExpression::Case(e) => CompiledExpressionState::Case(CaseExpressionState {
                check: Box::new(Self::initialize(&e.check)),
                result_if_true: Box::new(Self::initialize(&e.result_if_true)),
                result_if_false: Box::new(Self::initialize(&e.result_if_false)),
                check_result: ValueSlot::default(),
                true_result: ValueSlot::default(),
                false_result: ValueSlot::default(),
                result: ValueSlot::default(),
            }),
            PhysicalExpression::Operator(e) => {
                CompiledExpressionState::Operator(OperatorExpressionState {
                    child_states: e.children.iter().map(Self::initialize).collect(),
                    child_results: (0..e.children.len())
                        .map(|_| ValueSlot::default())
                        .collect(),
                    in_list: Self::prepare_in_list(e),
                    like_pattern: Self::prepare_like_pattern(e),
                    result: ValueSlot::default(),
                    aux: ValueSlot::default(),
                    scratch: ValueSlot::default(),
                })
            }
            PhysicalExpression::Constant(_) => {
                CompiledExpressionState::Constant(ConstantExpressionState)
            }
            PhysicalExpression::Parameter(_) => {
                CompiledExpressionState::Parameter(ParameterExpressionState::default())
            }
            PhysicalExpression::ColumnRef(_) => {
                CompiledExpressionState::ColumnRef(ColumnRefExpressionState)
            }
            PhysicalExpression::Reference(_) => {
                CompiledExpressionState::Reference(ReferenceExpressionState)
            }
            PhysicalExpression::Shared(_) => CompiledExpressionState::Shared(SharedExpressionState),
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
        self.execute_kernel_into(
            expr_idx,
            VectorKernelInput::from_chunk(input)
                .with_selection(sel)
                .with_count(count),
            runtime,
            result,
        )
    }

    pub fn execute_all_into(
        &mut self,
        input: &Chunk,
        runtime: &dyn FunctionExecContext,
        result: &mut Chunk,
    ) -> Result<()> {
        self.execute_all_kernel(VectorKernelInput::from_chunk(input), runtime, result)
    }

    pub fn execute_all_kernel(
        &mut self,
        input: VectorKernelInput<'_>,
        runtime: &dyn FunctionExecContext,
        result: &mut Chunk,
    ) -> Result<()> {
        let physical = &self.program.physical;
        let output_types = physical.root_return_types();
        // A failed execution also releases its transient references before
        // returning, so this is only a defensive cleanup for callers that
        // abandon a partially evaluated batch through unwinding.
        self.state.release_batch_references();
        Self::prepare_output_chunk(
            result,
            &output_types,
            input.count,
            runtime.allocator(MemoryTag::BaseTable),
        )?;

        let execution = {
            let CompiledExecutorState {
                states,
                shared_states,
                shared_slots,
                batch_epoch,
            } = &mut self.state;
            *batch_epoch = batch_epoch.wrapping_add(1);
            let mut shared = SharedEvaluation {
                nodes: physical.shared_nodes(),
                states: shared_states,
                slots: shared_slots,
                epoch: *batch_epoch,
            };
            (|| {
                let mut fused_outputs = FusedOutputSet::new(physical.root_count());
                for chain in physical.decimal_factor_chains() {
                    if !fused_outputs
                        .pair_is_available(chain.producer_output, chain.consumer_output)
                    {
                        continue;
                    }
                    if Self::try_execute_decimal_factor_chain(
                        chain,
                        physical,
                        states,
                        input,
                        runtime,
                        result,
                        &mut shared,
                    )? {
                        fused_outputs.mark_pair(chain.producer_output, chain.consumer_output);
                    }
                }
                for expr_idx in 0..physical.root_count() {
                    if fused_outputs.contains(expr_idx) {
                        continue;
                    }
                    let first_output = physical.root_first_output(expr_idx);
                    if first_output < expr_idx {
                        result.data[expr_idx] = Arc::clone(&result.data[first_output]);
                        continue;
                    }
                    let column = result.column_mut(expr_idx).ok_or_else(|| {
                        paro_error::internal(format!("Output column {} not found", expr_idx))
                    })?;
                    let state_idx = physical.root_state_index(expr_idx);
                    let expr = physical.root(expr_idx);
                    if let PhysicalExpression::Shared(expr) = expr {
                        shared.execute_into(
                            expr,
                            input.columns,
                            input.selection,
                            input.count,
                            runtime,
                            input.params,
                            column,
                        )?;
                    } else {
                        Self::execute_into_inner(
                            expr,
                            &mut states[state_idx],
                            input.columns,
                            input.selection,
                            input.count,
                            runtime,
                            input.params,
                            column,
                            &mut shared,
                        )?;
                    }
                }
                result.try_set_cardinality(input.count)
            })()
        };
        self.state.release_batch_references();
        execution
    }

    pub fn execute_kernel_into(
        &mut self,
        expr_idx: usize,
        input: VectorKernelInput<'_>,
        runtime: &dyn FunctionExecContext,
        result: &mut Vector,
    ) -> Result<()> {
        let physical = &self.program.physical;
        let state_idx = physical.root_state_index(expr_idx);
        let expr = physical.root(expr_idx);
        let execution = {
            let CompiledExecutorState {
                states,
                shared_states,
                shared_slots,
                batch_epoch,
            } = &mut self.state;
            *batch_epoch = batch_epoch.wrapping_add(1);
            let mut shared = SharedEvaluation {
                nodes: physical.shared_nodes(),
                states: shared_states,
                slots: shared_slots,
                epoch: *batch_epoch,
            };
            Self::execute_into_inner(
                expr,
                &mut states[state_idx],
                input.columns,
                input.selection,
                input.count,
                runtime,
                input.params,
                result,
                &mut shared,
            )
        };
        self.state.release_batch_references();
        execution
    }

    pub fn select_into(
        &mut self,
        expr_idx: usize,
        input: &Chunk,
        count: usize,
        runtime: &dyn FunctionExecContext,
        sel: &mut SelectionVector,
    ) -> Result<usize> {
        self.select_kernel(
            expr_idx,
            VectorKernelInput::from_chunk(input).with_count(count),
            runtime,
            sel,
        )
    }

    pub fn select_kernel(
        &mut self,
        expr_idx: usize,
        input: VectorKernelInput<'_>,
        runtime: &dyn FunctionExecContext,
        sel: &mut SelectionVector,
    ) -> Result<usize> {
        let physical = &self.program.physical;
        let state_idx = physical.root_state_index(expr_idx);
        let expr = physical.root(expr_idx);
        let execution = {
            let CompiledExecutorState {
                states,
                shared_states,
                shared_slots,
                batch_epoch,
            } = &mut self.state;
            *batch_epoch = batch_epoch.wrapping_add(1);
            let mut shared = SharedEvaluation {
                nodes: physical.shared_nodes(),
                states: shared_states,
                slots: shared_slots,
                epoch: *batch_epoch,
            };
            Self::select_expression(
                expr,
                &mut states[state_idx],
                input.columns,
                input.selection,
                input.count,
                runtime,
                input.params,
                sel,
                &mut shared,
            )
        };
        self.state.release_batch_references();
        execution
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
            self.program.physical.root_return_type(expr_idx),
            count.max(1),
            runtime.allocator(MemoryTag::BaseTable),
        )?;
        self.execute_kernel_into(
            expr_idx,
            VectorKernelInput::from_chunk(chunk)
                .with_selection(sel)
                .with_count(count),
            runtime,
            &mut result,
        )?;
        Ok(Arc::new(result))
    }

    fn execute_into_inner(
        expr: &PhysicalExpression,
        state: &mut CompiledExpressionState,
        chunk: &Chunk,
        sel: Option<&SelectionVector>,
        count: usize,
        runtime: &dyn FunctionExecContext,
        params: Option<&ParameterBindings>,
        result: &mut Vector,
        shared: &mut SharedEvaluation<'_>,
    ) -> Result<()> {
        match (expr, &mut *state) {
            (PhysicalExpression::Function(expr), CompiledExpressionState::Function(state)) => {
                Self::execute_function_into(
                    expr, state, chunk, sel, count, runtime, params, result, shared,
                )
            }
            (PhysicalExpression::Cast(expr), CompiledExpressionState::Cast(state)) => {
                Self::execute_cast_into(
                    expr, state, chunk, sel, count, runtime, params, result, shared,
                )
            }
            (PhysicalExpression::Comparison(expr), CompiledExpressionState::Comparison(state)) => {
                Self::execute_comparison_into(
                    expr, state, chunk, sel, count, runtime, params, result, shared,
                )
            }
            (
                PhysicalExpression::Conjunction(expr),
                CompiledExpressionState::Conjunction(state),
            ) => Self::execute_conjunction_into(
                expr, state, chunk, sel, count, runtime, params, result, shared,
            ),
            (PhysicalExpression::Case(expr), CompiledExpressionState::Case(state)) => {
                Self::execute_case_into(
                    expr, state, chunk, sel, count, runtime, params, result, shared,
                )
            }
            (PhysicalExpression::Operator(expr), CompiledExpressionState::Operator(state)) => {
                Self::execute_operator_into(
                    expr, state, chunk, sel, count, runtime, params, result, shared,
                )
            }
            (PhysicalExpression::Constant(expr), CompiledExpressionState::Constant(_)) => {
                Self::execute_constant_into(expr, count, runtime, result)
            }
            (PhysicalExpression::Parameter(expr), CompiledExpressionState::Parameter(state)) => {
                Self::execute_parameter_into(&expr.slot, state, count, runtime, params, result)
            }
            (PhysicalExpression::ColumnRef(expr), CompiledExpressionState::ColumnRef(_)) => {
                Self::execute_column_ref_into(expr, chunk, sel, count, result)
            }
            (PhysicalExpression::Reference(expr), CompiledExpressionState::Reference(_)) => {
                Self::execute_reference_into(expr, chunk, sel, count, result)
            }
            (PhysicalExpression::Shared(expr), CompiledExpressionState::Shared(_)) => {
                let value = shared.execute_value(expr, chunk, sel, count, runtime, params)?;
                value.write_into(result)?;
                result.set_len(count);
                Ok(())
            }
            _ => Err(expression_state_mismatch(expr, state)),
        }
    }

    fn execute_value(
        expr: &PhysicalExpression,
        state: &mut CompiledExpressionState,
        chunk: &Chunk,
        sel: Option<&SelectionVector>,
        count: usize,
        runtime: &dyn FunctionExecContext,
        params: Option<&ParameterBindings>,
        shared: &mut SharedEvaluation<'_>,
    ) -> Result<EvaluatedValue> {
        match (expr, &mut *state) {
            (PhysicalExpression::Function(expr), CompiledExpressionState::Function(state)) => {
                Self::execute_function_value(
                    expr, state, chunk, sel, count, runtime, params, shared,
                )
            }
            (PhysicalExpression::Cast(expr), CompiledExpressionState::Cast(state)) => {
                Self::execute_cast_value(expr, state, chunk, sel, count, runtime, params, shared)
            }
            (PhysicalExpression::Comparison(expr), CompiledExpressionState::Comparison(state)) => {
                Self::execute_comparison_value(
                    expr, state, chunk, sel, count, runtime, params, shared,
                )
            }
            (
                PhysicalExpression::Conjunction(expr),
                CompiledExpressionState::Conjunction(state),
            ) => Self::execute_conjunction_value(
                expr, state, chunk, sel, count, runtime, params, shared,
            ),
            (PhysicalExpression::Case(expr), CompiledExpressionState::Case(state)) => {
                Self::execute_case_value(expr, state, chunk, sel, count, runtime, params, shared)
            }
            (PhysicalExpression::Operator(expr), CompiledExpressionState::Operator(state)) => {
                Self::execute_operator_value(
                    expr, state, chunk, sel, count, runtime, params, shared,
                )
            }
            (PhysicalExpression::Constant(expr), CompiledExpressionState::Constant(_)) => {
                Ok(EvaluatedValue::Borrowed(Vector::try_constant_from_value(
                    expr.return_type.clone(),
                    expr.value.clone(),
                    count,
                    runtime.allocator(MemoryTag::BaseTable),
                )?))
            }
            (PhysicalExpression::Parameter(expr), CompiledExpressionState::Parameter(state)) => {
                Self::execute_parameter_value(&expr.slot, state, count, runtime, params)
            }
            (PhysicalExpression::ColumnRef(expr), CompiledExpressionState::ColumnRef(_)) => {
                Self::execute_column_ref_value(expr, chunk, sel, count)
            }
            (PhysicalExpression::Reference(expr), CompiledExpressionState::Reference(_)) => {
                Self::execute_reference_value(expr, chunk, sel, count)
            }
            (PhysicalExpression::Shared(expr), CompiledExpressionState::Shared(_)) => {
                shared.execute_value(expr, chunk, sel, count, runtime, params)
            }
            _ => Err(expression_state_mismatch(expr, state)),
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

    /// Prepare the argument container for a function invocation.
    ///
    /// Its columns are borrowed references to child results and are replaced
    /// in full before the function runs. Resetting the previous columns would
    /// make those shared vectors exclusive (and therefore copy their complete
    /// buffers) only to drop them immediately afterwards.
    fn prepare_intermediate_chunk<'a>(
        intermediate_types: &[LogicalType],
        intermediate_chunk: &'a mut Option<Chunk>,
        count: usize,
        allocator: Arc<dyn Allocator>,
    ) -> Result<&'a mut Chunk> {
        let required_capacity = count.max(1);
        let needs_reinit = intermediate_chunk
            .as_ref()
            .is_none_or(|chunk| chunk.capacity() < required_capacity);
        if needs_reinit {
            let mut chunk = Chunk::try_new(allocator)?;
            chunk.set_capacity(required_capacity);
            chunk.data.reserve(intermediate_types.len());
            *intermediate_chunk = Some(chunk);
        } else if let Some(chunk) = intermediate_chunk.as_mut() {
            chunk.clear_columns();
        }
        let intermediate = intermediate_chunk
            .as_mut()
            .expect("intermediate chunk initialized");
        // Cardinality is independent of physical columns. Zero-argument
        // functions still receive one logical input row in scalar projections.
        intermediate.try_set_cardinality(count)?;
        Ok(intermediate)
    }

    fn store_value(slot: &mut ValueSlot, value: &EvaluatedValue) {
        slot.set_value(value.as_vector().reference());
    }

    fn parameter_bindings(params: Option<&ParameterBindings>) -> Result<&ParameterBindings> {
        params.ok_or_else(|| {
            paro_error::internal("parameter expression evaluated without ParameterBindings")
        })
    }

    fn execute_parameter_into(
        slot: &ParameterSlot,
        state: &mut ParameterExpressionState,
        count: usize,
        runtime: &dyn FunctionExecContext,
        params: Option<&ParameterBindings>,
        result: &mut Vector,
    ) -> Result<()> {
        let value = Self::execute_parameter_value(slot, state, count, runtime, params)?;
        value.write_into(result)?;
        result.set_len(count);
        Ok(())
    }

    fn execute_parameter_value(
        slot: &ParameterSlot,
        state: &mut ParameterExpressionState,
        count: usize,
        runtime: &dyn FunctionExecContext,
        params: Option<&ParameterBindings>,
    ) -> Result<EvaluatedValue> {
        let params = Self::parameter_bindings(params)?;
        let bound = params.value_for_slot(slot)?;
        let needs_refresh = state.cached_epoch != Some(params.epoch())
            || state
                .result
                .as_ref()
                .is_none_or(|vector| vector.len() != count || vector.logical_type() != &slot.ty);
        if needs_refresh {
            state.result.set_value(Vector::try_constant_from_value(
                slot.ty.clone(),
                bound.clone(),
                count,
                runtime.allocator(MemoryTag::BaseTable),
            )?);
            state.cached_epoch = Some(params.epoch());
        }
        state
            .result
            .evaluated(false)
            .ok_or_else(|| paro_error::internal("parameter result slot was not initialized"))
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
        expr: &PhysicalFunctionExpression,
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

    fn prepare_in_list(expr: &PhysicalOperatorExpression) -> Option<PreparedInList> {
        if !matches!(expr.operator_type, OperatorType::In | OperatorType::NotIn)
            || expr.children.len() < 2
        {
            return None;
        }

        let mut values = Vec::with_capacity(expr.children.len().saturating_sub(1));
        let mut has_null = false;
        for child in &expr.children[1..] {
            let PhysicalExpression::Constant(constant) = child else {
                return Some(PreparedInList::Dynamic);
            };
            if constant.value.is_null() {
                has_null = true;
            } else {
                values.push(constant.value.clone());
            }
        }

        match expr.children[0].return_type() {
            LogicalType::Integer => {
                if let Some(values) = Self::prepare_i32_in_values(&values) {
                    return Some(PreparedInList::I32Const { values, has_null });
                }
            }
            LogicalType::BigInt => {
                if let Some(values) = Self::prepare_i64_in_values(&values) {
                    return Some(PreparedInList::I64Const { values, has_null });
                }
            }
            _ => {}
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

    fn prepare_like_pattern(expr: &PhysicalOperatorExpression) -> Option<PreparedLikePattern> {
        if !matches!(expr.operator_type, OperatorType::Like | OperatorType::ILike) {
            return None;
        }
        let PhysicalExpression::Constant(pattern) = expr.children.get(1)? else {
            return None;
        };
        let Value::Varchar(pattern) = &pattern.value else {
            return None;
        };
        PreparedLikePattern::try_new(pattern, matches!(expr.operator_type, OperatorType::ILike))
    }

    fn prepare_i32_in_values(values: &[Value]) -> Option<Vec<i32>> {
        let mut typed = values
            .iter()
            .map(Value::as_i64)
            .map(|value| value.and_then(|value| i32::try_from(value).ok()))
            .collect::<Option<Vec<_>>>()?;
        typed.sort_unstable();
        typed.dedup();
        Some(typed)
    }

    fn prepare_i64_in_values(values: &[Value]) -> Option<Vec<i64>> {
        let mut typed = values
            .iter()
            .map(Value::as_i64)
            .collect::<Option<Vec<_>>>()?;
        typed.sort_unstable();
        typed.dedup();
        Some(typed)
    }

    fn select_expression(
        expr: &PhysicalExpression,
        state: &mut CompiledExpressionState,
        chunk: &Chunk,
        input_sel: Option<&SelectionVector>,
        count: usize,
        runtime: &dyn FunctionExecContext,
        params: Option<&ParameterBindings>,
        output_sel: &mut SelectionVector,
        shared: &mut SharedEvaluation<'_>,
    ) -> Result<usize> {
        if let Some(selected) = Self::try_direct_select(
            expr, state, chunk, input_sel, count, runtime, params, output_sel, shared,
        )? {
            return Ok(selected);
        }

        let value = Self::execute_value(
            expr, state, chunk, input_sel, count, runtime, params, shared,
        )?;
        Ok(scan_bool_selection(
            value.as_vector(),
            input_sel,
            count,
            output_sel,
        ))
    }

    fn try_direct_select(
        expr: &PhysicalExpression,
        state: &mut CompiledExpressionState,
        chunk: &Chunk,
        input_sel: Option<&SelectionVector>,
        count: usize,
        runtime: &dyn FunctionExecContext,
        params: Option<&ParameterBindings>,
        output_sel: &mut SelectionVector,
        shared: &mut SharedEvaluation<'_>,
    ) -> Result<Option<usize>> {
        match (expr, state) {
            (PhysicalExpression::Comparison(expr), CompiledExpressionState::Comparison(state)) => {
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
                    params,
                    shared,
                )?;
                let right = Self::execute_value(
                    &expr.right,
                    &mut state.right,
                    chunk,
                    input_sel,
                    count,
                    runtime,
                    params,
                    shared,
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
            (
                PhysicalExpression::Conjunction(expr),
                CompiledExpressionState::Conjunction(state),
            ) => {
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
                                params,
                                &mut next,
                                shared,
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
                                params,
                                &mut next,
                                shared,
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
            (PhysicalExpression::Operator(expr), CompiledExpressionState::Operator(state)) => {
                match expr.operator_type {
                    OperatorType::IsNull => {
                        let child = Self::execute_value(
                            &expr.children[0],
                            &mut state.child_states[0],
                            chunk,
                            input_sel,
                            count,
                            runtime,
                            params,
                            shared,
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
                            params,
                            shared,
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
                            params,
                            shared,
                        )?;
                        Self::store_value(&mut state.child_results[0], &child);
                        Ok(Some(scan_false_bool_selection(
                            child.as_vector(),
                            input_sel,
                            count,
                            output_sel,
                        )))
                    }
                    OperatorType::Like | OperatorType::ILike => {
                        let Some(pattern) = state.like_pattern.as_ref() else {
                            return Ok(None);
                        };
                        let value = Self::execute_value(
                            &expr.children[0],
                            &mut state.child_states[0],
                            chunk,
                            input_sel,
                            count,
                            runtime,
                            params,
                            shared,
                        )?;
                        Self::store_value(&mut state.child_results[0], &value);
                        Ok(Some(select_prepared_like(
                            value.as_vector(),
                            pattern,
                            input_sel,
                            count,
                            output_sel,
                        )?))
                    }
                    _ => Ok(None),
                }
            }
            (PhysicalExpression::Constant(expr), CompiledExpressionState::Constant(_))
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
            (PhysicalExpression::ColumnRef(expr), CompiledExpressionState::ColumnRef(_))
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
            (PhysicalExpression::Reference(expr), CompiledExpressionState::Reference(_))
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
            (PhysicalExpression::Parameter(expr), CompiledExpressionState::Parameter(state))
                if expr.slot.ty == LogicalType::Boolean =>
            {
                let value =
                    Self::execute_parameter_value(&expr.slot, state, count, runtime, params)?;
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
        expr: &PhysicalFunctionExpression,
        state: &mut ExecuteFunctionState,
        chunk: &Chunk,
        sel: Option<&SelectionVector>,
        count: usize,
        runtime: &dyn FunctionExecContext,
        params: Option<&ParameterBindings>,
        result: &mut Vector,
        shared: &mut SharedEvaluation<'_>,
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
        for (child_expr, child_state) in expr.children.iter().zip(child_states.iter_mut()) {
            let child_value = Self::execute_value(
                child_expr,
                child_state,
                chunk,
                sel,
                count,
                runtime,
                params,
                shared,
            )?;
            intermediate.try_push_column(Arc::new(child_value.as_vector().reference()), count)?;
        }
        debug_assert_eq!(intermediate.column_count(), intermediate_types.len());
        Self::ensure_function_local_state(&expr.function, local_state, runtime)?;
        let cached_result = Self::try_dictionary_cached_function(
            expr,
            intermediate,
            count,
            runtime,
            allocator.clone(),
            local_state.as_deref(),
            cached_dictionary_input_id,
            cached_dictionary_output,
        );
        let cached_result = match cached_result {
            Ok(result) => result,
            Err(error) => {
                intermediate.clear_columns();
                return Err(error);
            }
        };
        if let Some(cached_result) = cached_result {
            intermediate.clear_columns();
            *result = cached_result;
            return Ok(());
        }
        Self::prepare_result_vector(result, &expr.return_type, count, allocator)?;
        let function_context = BoundFunctionContext::new(
            runtime,
            expr.function.bind_data.as_deref(),
            local_state.as_deref(),
        );
        let execution = expr
            .function
            .execute(intermediate, &function_context, result);
        intermediate.clear_columns();
        execution
    }

    fn execute_function_value(
        expr: &PhysicalFunctionExpression,
        state: &mut ExecuteFunctionState,
        chunk: &Chunk,
        sel: Option<&SelectionVector>,
        count: usize,
        runtime: &dyn FunctionExecContext,
        params: Option<&ParameterBindings>,
        shared: &mut SharedEvaluation<'_>,
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
        for (child_expr, child_state) in expr.children.iter().zip(child_states.iter_mut()) {
            let child_value = Self::execute_value(
                child_expr,
                child_state,
                chunk,
                sel,
                count,
                runtime,
                params,
                shared,
            )?;
            intermediate.try_push_column(Arc::new(child_value.as_vector().reference()), count)?;
        }
        debug_assert_eq!(intermediate.column_count(), intermediate_types.len());
        Self::ensure_function_local_state(&expr.function, local_state, runtime)?;
        let cached_result = Self::try_dictionary_cached_function(
            expr,
            intermediate,
            count,
            runtime,
            allocator.clone(),
            local_state.as_deref(),
            cached_dictionary_input_id,
            cached_dictionary_output,
        );
        let cached_result = match cached_result {
            Ok(result) => result,
            Err(error) => {
                intermediate.clear_columns();
                return Err(error);
            }
        };
        if let Some(cached_result) = cached_result {
            intermediate.clear_columns();
            return Ok(EvaluatedValue::Borrowed(cached_result));
        }
        let result_vector = Self::prepare_slot_result(result, &expr.return_type, count, allocator)?;
        let function_context = BoundFunctionContext::new(
            runtime,
            expr.function.bind_data.as_deref(),
            local_state.as_deref(),
        );
        let execution = expr
            .function
            .execute(intermediate, &function_context, result_vector);
        intermediate.clear_columns();
        execution?;
        Ok(result.evaluated(true).expect("function result initialized"))
    }

    fn execute_cast_into(
        expr: &PhysicalCastExpression,
        state: &mut CastExpressionState,
        chunk: &Chunk,
        sel: Option<&SelectionVector>,
        count: usize,
        runtime: &dyn FunctionExecContext,
        params: Option<&ParameterBindings>,
        result: &mut Vector,
        shared: &mut SharedEvaluation<'_>,
    ) -> Result<()> {
        let child_value = Self::execute_value(
            &expr.child,
            &mut state.child,
            chunk,
            sel,
            count,
            runtime,
            params,
            shared,
        )?;
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
        expr: &PhysicalCastExpression,
        state: &mut CastExpressionState,
        chunk: &Chunk,
        sel: Option<&SelectionVector>,
        count: usize,
        runtime: &dyn FunctionExecContext,
        params: Option<&ParameterBindings>,
        shared: &mut SharedEvaluation<'_>,
    ) -> Result<EvaluatedValue> {
        let child_value = Self::execute_value(
            &expr.child,
            &mut state.child,
            chunk,
            sel,
            count,
            runtime,
            params,
            shared,
        )?;
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
        expr: &PhysicalComparisonExpression,
        state: &mut ComparisonExpressionState,
        chunk: &Chunk,
        sel: Option<&SelectionVector>,
        count: usize,
        runtime: &dyn FunctionExecContext,
        params: Option<&ParameterBindings>,
        result: &mut Vector,
        shared: &mut SharedEvaluation<'_>,
    ) -> Result<()> {
        let left = Self::execute_value(
            &expr.left,
            &mut state.left,
            chunk,
            sel,
            count,
            runtime,
            params,
            shared,
        )?;
        let right = Self::execute_value(
            &expr.right,
            &mut state.right,
            chunk,
            sel,
            count,
            runtime,
            params,
            shared,
        )?;
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
        expr: &PhysicalComparisonExpression,
        state: &mut ComparisonExpressionState,
        chunk: &Chunk,
        sel: Option<&SelectionVector>,
        count: usize,
        runtime: &dyn FunctionExecContext,
        params: Option<&ParameterBindings>,
        shared: &mut SharedEvaluation<'_>,
    ) -> Result<EvaluatedValue> {
        let left = Self::execute_value(
            &expr.left,
            &mut state.left,
            chunk,
            sel,
            count,
            runtime,
            params,
            shared,
        )?;
        let right = Self::execute_value(
            &expr.right,
            &mut state.right,
            chunk,
            sel,
            count,
            runtime,
            params,
            shared,
        )?;
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
        expr: &PhysicalConjunctionExpression,
        state: &mut ConjunctionExpressionState,
        chunk: &Chunk,
        sel: Option<&SelectionVector>,
        count: usize,
        runtime: &dyn FunctionExecContext,
        params: Option<&ParameterBindings>,
        result: &mut Vector,
        shared: &mut SharedEvaluation<'_>,
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
            params,
            shared,
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
                params,
                shared,
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
        expr: &PhysicalConjunctionExpression,
        state: &mut ConjunctionExpressionState,
        chunk: &Chunk,
        sel: Option<&SelectionVector>,
        count: usize,
        runtime: &dyn FunctionExecContext,
        params: Option<&ParameterBindings>,
        shared: &mut SharedEvaluation<'_>,
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
            params,
            shared,
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
                params,
                shared,
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
        expr: &PhysicalCaseExpression,
        state: &mut CaseExpressionState,
        chunk: &Chunk,
        sel: Option<&SelectionVector>,
        count: usize,
        runtime: &dyn FunctionExecContext,
        params: Option<&ParameterBindings>,
        result: &mut Vector,
        shared: &mut SharedEvaluation<'_>,
    ) -> Result<()> {
        let check = Self::execute_value(
            &expr.check,
            &mut state.check,
            chunk,
            sel,
            count,
            runtime,
            params,
            shared,
        )?;
        let if_true = Self::execute_value(
            &expr.result_if_true,
            &mut state.result_if_true,
            chunk,
            sel,
            count,
            runtime,
            params,
            shared,
        )?;
        let if_false = Self::execute_value(
            &expr.result_if_false,
            &mut state.result_if_false,
            chunk,
            sel,
            count,
            runtime,
            params,
            shared,
        )?;
        Self::store_value(&mut state.check_result, &check);
        Self::store_value(&mut state.true_result, &if_true);
        Self::store_value(&mut state.false_result, &if_false);
        let merged = paro_function::scalar::operators::case::CaseExecutor::execute(
            check.as_vector(),
            if_true.as_vector(),
            if_false.as_vector(),
            count,
            result.allocator().clone(),
        )?;
        *result = merged;
        Ok(())
    }

    fn execute_case_value(
        expr: &PhysicalCaseExpression,
        state: &mut CaseExpressionState,
        chunk: &Chunk,
        sel: Option<&SelectionVector>,
        count: usize,
        runtime: &dyn FunctionExecContext,
        params: Option<&ParameterBindings>,
        shared: &mut SharedEvaluation<'_>,
    ) -> Result<EvaluatedValue> {
        let check = Self::execute_value(
            &expr.check,
            &mut state.check,
            chunk,
            sel,
            count,
            runtime,
            params,
            shared,
        )?;
        let if_true = Self::execute_value(
            &expr.result_if_true,
            &mut state.result_if_true,
            chunk,
            sel,
            count,
            runtime,
            params,
            shared,
        )?;
        let if_false = Self::execute_value(
            &expr.result_if_false,
            &mut state.result_if_false,
            chunk,
            sel,
            count,
            runtime,
            params,
            shared,
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
                runtime.allocator(MemoryTag::BaseTable),
            )?,
        );
        Ok(state
            .result
            .evaluated(true)
            .expect("case result initialized"))
    }

    fn execute_operator_into(
        expr: &PhysicalOperatorExpression,
        state: &mut OperatorExpressionState,
        chunk: &Chunk,
        sel: Option<&SelectionVector>,
        count: usize,
        runtime: &dyn FunctionExecContext,
        params: Option<&ParameterBindings>,
        result: &mut Vector,
        shared: &mut SharedEvaluation<'_>,
    ) -> Result<()> {
        let value =
            Self::execute_operator_value(expr, state, chunk, sel, count, runtime, params, shared)?;
        value.write_into(result)?;
        result.set_len(count);
        Ok(())
    }

    fn execute_operator_value(
        expr: &PhysicalOperatorExpression,
        state: &mut OperatorExpressionState,
        chunk: &Chunk,
        sel: Option<&SelectionVector>,
        count: usize,
        runtime: &dyn FunctionExecContext,
        params: Option<&ParameterBindings>,
        shared: &mut SharedEvaluation<'_>,
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
                    params,
                    shared,
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
                    params,
                    shared,
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
                        params,
                        shared,
                    )?;
                    Self::store_value(&mut state.child_results[child_idx], &child);

                    let next_count = apply_coalesce_child(
                        result,
                        child.as_vector(),
                        &unresolved,
                        unresolved_count,
                        &mut next_unresolved,
                    )?;
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
                    params,
                    shared,
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
                    PreparedInList::I32Const { values, has_null } => {
                        Self::execute_in_i32_const(
                            lhs.as_vector(),
                            values,
                            *has_null,
                            negate,
                            result,
                            count,
                        );
                    }
                    PreparedInList::I64Const { values, has_null } => {
                        Self::execute_in_i64_const(
                            lhs.as_vector(),
                            values,
                            *has_null,
                            negate,
                            result,
                            count,
                        );
                    }
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
                                params,
                                shared,
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
            OperatorType::Like | OperatorType::ILike => {
                let value = Self::execute_value(
                    &expr.children[0],
                    &mut state.child_states[0],
                    chunk,
                    sel,
                    count,
                    runtime,
                    params,
                    shared,
                )?;
                Self::store_value(&mut state.child_results[0], &value);

                let case_insensitive = matches!(expr.operator_type, OperatorType::ILike);
                if let Some(pattern) = state.like_pattern.as_ref() {
                    let result = Self::prepare_slot_result(
                        &mut state.result,
                        &LogicalType::Boolean,
                        count,
                        runtime.allocator(MemoryTag::BaseTable),
                    )?;
                    let values = value.as_vector().try_to_utf8_view(count)?;
                    for row_idx in 0..count {
                        if !values.is_valid(row_idx) {
                            result.set_null(row_idx, true);
                            continue;
                        }
                        result.set_bool(row_idx, pattern.matches(values.str(row_idx)));
                    }
                } else {
                    let pattern = Self::execute_value(
                        &expr.children[1],
                        &mut state.child_states[1],
                        chunk,
                        sel,
                        count,
                        runtime,
                        params,
                        shared,
                    )?;
                    Self::store_value(&mut state.child_results[1], &pattern);
                    let result = Self::prepare_slot_result(
                        &mut state.result,
                        &LogicalType::Boolean,
                        count,
                        runtime.allocator(MemoryTag::BaseTable),
                    )?;
                    let values = value.as_vector().try_to_utf8_view(count)?;
                    let patterns = pattern.as_vector().try_to_utf8_view(count)?;
                    for row_idx in 0..count {
                        if !values.is_valid(row_idx) || !patterns.is_valid(row_idx) {
                            result.set_null(row_idx, true);
                            continue;
                        }
                        result.set_bool(
                            row_idx,
                            sql_like(values.str(row_idx), patterns.str(row_idx), case_insensitive),
                        );
                    }
                }

                Ok(state
                    .result
                    .evaluated(true)
                    .expect("like result initialized"))
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
                        params,
                        shared,
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
                        params,
                        shared,
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
                    params,
                    shared,
                )?;
                let row_count = Self::execute_value(
                    &expr.children[1],
                    &mut state.child_states[1],
                    chunk,
                    sel,
                    count,
                    runtime,
                    params,
                    shared,
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
            OperatorType::ArrayExtract => Err(paro_error::not_implemented(format!(
                "{:?} operator",
                expr.operator_type
            ))),
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

    fn execute_in_i32_const(
        lhs: &Vector,
        values: &[i32],
        has_null_rhs: bool,
        negate: bool,
        result: &mut Vector,
        count: usize,
    ) {
        for row_idx in 0..count {
            let Some(lhs_value) = lhs.get_i32(row_idx) else {
                result.set_null(row_idx, true);
                continue;
            };

            if values.binary_search(&lhs_value).is_ok() {
                result.set_bool(row_idx, !negate);
            } else if has_null_rhs {
                result.set_null(row_idx, true);
            } else {
                result.set_bool(row_idx, negate);
            }
        }
    }

    fn execute_in_i64_const(
        lhs: &Vector,
        values: &[i64],
        has_null_rhs: bool,
        negate: bool,
        result: &mut Vector,
        count: usize,
    ) {
        for row_idx in 0..count {
            let Some(lhs_value) = lhs.get_i64(row_idx) else {
                result.set_null(row_idx, true);
                continue;
            };

            if values.binary_search(&lhs_value).is_ok() {
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
        expr: &super::program::ExpressionConstant,
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
        expr: &PhysicalColumnRefExpression,
        chunk: &Chunk,
        sel: Option<&SelectionVector>,
        count: usize,
    ) -> Result<EvaluatedValue> {
        if expr.column_index >= chunk.data.len() {
            if count == 0 {
                return Ok(EvaluatedValue::Borrowed(Vector::try_new(
                    expr.return_type.clone(),
                    0,
                    chunk.allocator().clone(),
                )?));
            }
            return Err(paro_error::internal(format!(
                "Column reference index {} out of bounds (chunk columns={})",
                expr.column_index,
                chunk.data.len()
            )));
        }
        let column = chunk.data[expr.column_index].as_ref();
        if let Some(sel) = sel {
            Ok(EvaluatedValue::Borrowed(Vector::try_dictionary(
                chunk.data[expr.column_index].clone(),
                sel,
            )?))
        } else {
            Ok(EvaluatedValue::Borrowed(column.reference()))
        }
    }

    fn execute_column_ref_into(
        expr: &PhysicalColumnRefExpression,
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
        expr: &PhysicalReferenceExpression,
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
        expr: &PhysicalReferenceExpression,
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

    #[test]
    fn like_matches_sql_wildcards_and_escapes() {
        assert!(sql_like("PROMO BURNISHED COPPER", "PROMO%", false));
        assert!(sql_like(
            "special instructions and requests",
            "%special%requests%",
            false
        ));
        assert!(sql_like("A_B", "A\\_B", false));
        assert!(sql_like("aXb", "A_B", true));
        assert!(!sql_like("BRASS PLATED", "%BRASS", false));
        assert!(sql_like("100%", "100\\%", false));
        assert!(!sql_like("100\\%", "100\\%", false));
        assert!(sql_like("你好世界", "你_世%", false));
        assert!(sql_like("Éclair", "é%", true));
        assert!(!sql_like("anything", "", false));
        assert!(sql_like("", "%", false));
    }

    #[test]
    fn like_handles_long_wildcard_patterns_in_linear_space() {
        let value = "a".repeat(8_192);
        let pattern = format!("%{}b", "a%".repeat(4_096));
        assert!(!sql_like(&value, &pattern, false));
    }
    use crate::memory_runtime::QueryMemoryPool;
    use crate::runtime::{
        ParameterBindingEpoch, ParameterBindings, QueryOutputPort, QueryRuntimeContext,
    };
    use paro_common::typed_parameters::{ParameterSlot, RuntimeParamId};
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
        OperatorExpression, OperatorType, ParameterExpression, ReferenceExpression,
        SubqueryExpression, SubqueryPlanningState, SubqueryType,
    };
    use paro_planner::operator::ColumnBinding;
    use paro_planner::operator::{ExpressionGet, LogicalOperator};
    use paro_planner::plan::{LogicalPlan, PlannedStatement};

    static LOCAL_STATE_INIT_COUNT: AtomicUsize = AtomicUsize::new(0);

    fn test_session() -> Arc<StatementContext> {
        TestStatementContextBuilder::minimal().build()
    }

    fn test_runtime(session: Arc<StatementContext>) -> QueryRuntimeContext {
        QueryRuntimeContext::new(
            session,
            Arc::new(ParameterBindings::empty()),
            Arc::new(QueryMemoryPool::unbounded()),
            QueryOutputPort::discarding(),
        )
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

    fn constant_varchar(value: &str) -> Expression {
        Expression::Constant(ConstantExpression::new(
            Value::Varchar(value.to_string()),
            LogicalType::Varchar,
        ))
    }

    fn parameter_i32(index: usize) -> Expression {
        Expression::Parameter(ParameterExpression::new(ParameterSlot::new(
            RuntimeParamId::new(index),
            LogicalType::Integer,
        )))
    }

    fn parameter_bool(index: usize) -> Expression {
        Expression::Parameter(ParameterExpression::new(ParameterSlot::new(
            RuntimeParamId::new(index),
            LogicalType::Boolean,
        )))
    }

    fn parameter_bindings(
        values: Vec<Value>,
        types: Vec<LogicalType>,
        epoch: u64,
    ) -> ParameterBindings {
        ParameterBindings::new(values, types, ParameterBindingEpoch::new(epoch))
            .expect("test parameter bindings should be valid")
    }

    fn reference_i32(index: usize) -> Expression {
        Expression::Reference(ReferenceExpression::new(index, LogicalType::Integer))
    }

    fn reference_i64(index: usize) -> Expression {
        Expression::Reference(ReferenceExpression::new(index, LogicalType::BigInt))
    }

    fn reference_varchar(index: usize) -> Expression {
        Expression::Reference(ReferenceExpression::new(index, LogicalType::Varchar))
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

    #[derive(Debug, Clone, PartialEq, Hash)]
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

        fn fingerprint(&self) -> u64 {
            paro_function::scalar::function_data_fingerprint(self)
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

        fn fingerprint(&self) -> u64 {
            Arc::as_ptr(&self.counter) as usize as u64
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
    fn parameter_expression_reads_epoch_scoped_bindings() {
        let session = test_session();
        let runtime = test_runtime(session);
        let expr = Expression::Comparison(ComparisonExpression::new(
            ComparisonType::GreaterThan,
            reference_i32(0),
            parameter_i32(0),
        ));
        let mut executor = ExpressionExecutor::new(&expr);
        let input = integer_chunk(&[1, 3, 5]);
        let first_bindings =
            parameter_bindings(vec![Value::Integer(2)], vec![LogicalType::Integer], 1);
        let second_bindings =
            parameter_bindings(vec![Value::Integer(4)], vec![LogicalType::Integer], 2);
        let mut first =
            paro_common::test_utils::test_vector_with_capacity(LogicalType::Boolean, input.size());
        let mut second =
            paro_common::test_utils::test_vector_with_capacity(LogicalType::Boolean, input.size());

        executor
            .execute_kernel_into(
                0,
                VectorKernelInput::from_eval_input(ExpressionEvalInput {
                    params: &first_bindings,
                    columns: &input,
                }),
                &runtime,
                &mut first,
            )
            .expect("parameter comparison should execute");
        executor
            .execute_kernel_into(
                0,
                VectorKernelInput::from_eval_input(ExpressionEvalInput {
                    params: &second_bindings,
                    columns: &input,
                }),
                &runtime,
                &mut second,
            )
            .expect("parameter comparison should refresh when epoch changes");

        assert_eq!(first.get_bool(0), Some(false));
        assert_eq!(first.get_bool(1), Some(true));
        assert_eq!(first.get_bool(2), Some(true));
        assert_eq!(second.get_bool(0), Some(false));
        assert_eq!(second.get_bool(1), Some(false));
        assert_eq!(second.get_bool(2), Some(true));
    }

    #[test]
    fn boolean_parameter_select_uses_direct_selection_path() {
        let session = test_session();
        let runtime = test_runtime(session);
        let expr = parameter_bool(0);
        let mut executor = ExpressionExecutor::new(&expr);
        let mut input = Chunk::try_new(paro_common::test_utils::test_allocator())
            .expect("test chunk allocation failed");
        input.set_cardinality(3);
        let true_bindings =
            parameter_bindings(vec![Value::Boolean(true)], vec![LogicalType::Boolean], 1);
        let false_bindings =
            parameter_bindings(vec![Value::Boolean(false)], vec![LogicalType::Boolean], 2);
        let mut selection = paro_common::test_utils::test_selection_with_capacity(input.size());

        let selected = executor
            .select_kernel(
                0,
                VectorKernelInput::from_eval_input(ExpressionEvalInput {
                    params: &true_bindings,
                    columns: &input,
                }),
                &runtime,
                &mut selection,
            )
            .expect("true parameter select should execute");
        assert_eq!(selected, 3);
        assert_eq!(selection.as_slice(), &[0, 1, 2]);

        let selected = executor
            .select_kernel(
                0,
                VectorKernelInput::from_eval_input(ExpressionEvalInput {
                    params: &false_bindings,
                    columns: &input,
                }),
                &runtime,
                &mut selection,
            )
            .expect("false parameter select should execute");
        assert_eq!(selected, 0);
        assert!(selection.as_slice().is_empty());
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
    fn in_list_uses_typed_constant_strategy_with_sql_null_semantics() {
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
                    Some(PreparedInList::I32Const { .. })
                ));
            }
            other => panic!("expected operator state, got {other:?}"),
        }
        match large_executor.compiled_state(0) {
            CompiledExpressionState::Operator(state) => {
                assert!(matches!(
                    state.in_list.as_ref(),
                    Some(PreparedInList::I32Const { .. })
                ));
            }
            other => panic!("expected operator state, got {other:?}"),
        }
    }

    #[test]
    fn in_list_keeps_boxed_hash_fallback_for_large_non_typed_constants() {
        let mut children = vec![reference_varchar(0)];
        for value in 0..16 {
            children.push(constant_varchar(&format!("value_{value}")));
        }
        let expr = Expression::Operator(OperatorExpression::new(
            OperatorType::In,
            children,
            LogicalType::Boolean,
        ));
        let executor = ExpressionExecutor::new(&expr);

        match executor.compiled_state(0) {
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
    fn function_intermediate_chunk_releases_borrows_and_reuses_capacity() {
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
        let first_column_capacity = first_chunk.data.capacity();
        assert_eq!(first_chunk.column_count(), 0);
        assert!(first_column_capacity >= 1);

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
        assert_eq!(reused_chunk.data.capacity(), first_column_capacity);
        assert_eq!(reused_chunk.column_count(), 0);

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
        assert!(expanded_chunk.data.capacity() >= 1);
        assert_eq!(expanded_chunk.column_count(), 0);
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
    fn zero_argument_functions_receive_the_input_cardinality_and_runtime_context() {
        let session = TestStatementContextBuilder::minimal()
            .with_current_user("alice")
            .build();
        let runtime = test_runtime(session);
        let function = BoundScalarFunction::from(
            paro_function::scalar::system::get_current_user_functions().functions[0].clone(),
        );
        let expression = Expression::Function(FunctionExpression::new(
            function,
            Vec::new(),
            LogicalType::Varchar,
        ));
        let mut executor = ExpressionExecutor::new(&expression);
        let mut input =
            Chunk::try_new(paro_common::test_utils::test_allocator()).expect("input chunk");
        input.set_cardinality(1);

        let result = executor
            .execute_expression(0, &input, None, 1, &runtime)
            .expect("zero-argument function execution");

        assert_eq!(result.len(), 1);
        assert_eq!(result.get_string(0), Some("alice"));
    }

    #[test]
    fn physical_program_exposes_version_and_stable_root_fingerprints() {
        let expr = greater_than_i32(0, 7);
        let executor = ExpressionExecutor::new(&expr);
        let program = executor.physical_program();

        assert_eq!(program.root_count(), 1);
        assert_eq!(
            program.root_fingerprints(),
            super::super::program::expression_list_fingerprints(std::slice::from_ref(&expr))
        );
        assert_eq!(
            program.version().backend,
            super::super::program::ExpressionBackend::VectorTreeV1
        );
    }

    #[test]
    fn duplicate_root_expressions_share_compiled_state_and_output() {
        let session = test_session();
        let runtime = test_runtime(session);
        let (expr, counter) = cached_identity_expr(0);
        let mut executor = ExpressionExecutor::with_expressions(&[expr.clone(), expr]);
        let mut output = Chunk::try_new(paro_common::test_utils::test_allocator())
            .expect("test chunk allocation failed");

        executor
            .execute_all_into(&integer_chunk(&[1, 2, 3]), &runtime, &mut output)
            .expect("duplicate roots should execute");

        assert_eq!(executor.physical_program().unique_root_count(), 1);
        assert_eq!(counter.load(Ordering::SeqCst), 1);
        assert!(Arc::ptr_eq(&output.data[0], &output.data[1]));
    }

    #[test]
    fn duplicate_leading_roots_do_not_shift_later_unique_state() {
        let session = test_session();
        let runtime = test_runtime(session);
        let duplicate = reference_i32(0);
        let later_unique = add_one_expr(0);
        let mut executor =
            ExpressionExecutor::with_expressions(&[duplicate.clone(), duplicate, later_unique]);
        let mut output = Chunk::try_new(paro_common::test_utils::test_allocator())
            .expect("test chunk allocation failed");

        executor
            .execute_all_into(&integer_chunk(&[1, 2, 3]), &runtime, &mut output)
            .expect("later unique root should keep its own compiled state");

        assert_eq!(executor.physical_program().unique_root_count(), 2);
        assert!(Arc::ptr_eq(&output.data[0], &output.data[1]));
        assert_eq!(output.get_value(2, 0), Some(Value::Integer(2)));
        assert_eq!(output.get_value(2, 2), Some(Value::Integer(4)));
    }

    #[test]
    fn repeated_child_expressions_share_scratch_within_kernel_batch() {
        let session = test_session();
        let runtime = test_runtime(session);
        let (shared_expr, counter) = cached_identity_expr(0);
        let greater = Expression::Comparison(ComparisonExpression::new(
            ComparisonType::GreaterThan,
            shared_expr.clone(),
            constant_i32(1),
        ));
        let less = Expression::Comparison(ComparisonExpression::new(
            ComparisonType::LessThan,
            shared_expr,
            constant_i32(4),
        ));
        let mut executor = ExpressionExecutor::with_expressions(&[greater, less]);
        let mut output = Chunk::try_new(paro_common::test_utils::test_allocator())
            .expect("test chunk allocation failed");

        assert_eq!(executor.physical_program().shared_expression_count(), 1);
        assert_eq!(executor.physical_program().scratch_layout().len(), 1);

        executor
            .execute_all_into(&integer_chunk(&[1, 2, 5]), &runtime, &mut output)
            .expect("shared child expressions should execute");

        assert_eq!(counter.load(Ordering::SeqCst), 1);
        let greater_result = output.column(0).expect("greater output");
        let less_result = output.column(1).expect("less output");
        assert_eq!(greater_result.get_bool(0), Some(false));
        assert_eq!(greater_result.get_bool(1), Some(true));
        assert_eq!(greater_result.get_bool(2), Some(true));
        assert_eq!(less_result.get_bool(0), Some(true));
        assert_eq!(less_result.get_bool(1), Some(true));
        assert_eq!(less_result.get_bool(2), Some(false));

        executor
            .execute_all_into(&integer_chunk(&[3]), &runtime, &mut output)
            .expect("next batch should recompute shared scratch");
        assert_eq!(counter.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn shared_expression_root_owns_and_reuses_its_output_allocation() {
        let session = test_session();
        let runtime = test_runtime(session);
        let (shared_expr, counter) = cached_identity_expr(0);
        let consumer = Expression::Comparison(ComparisonExpression::new(
            ComparisonType::GreaterThan,
            shared_expr.clone(),
            constant_i32(0),
        ));
        let mut executor = ExpressionExecutor::with_expressions(&[shared_expr, consumer]);
        let mut output = Chunk::try_new(paro_common::test_utils::test_allocator())
            .expect("test chunk allocation failed");

        executor
            .execute_all_into(&integer_chunk(&[1, 2, 3]), &runtime, &mut output)
            .expect("first shared root batch should execute");
        let mut first_allocations = Vec::new();
        output.data[0].collect_allocation_entries(&mut first_allocations);

        executor
            .execute_all_into(&integer_chunk(&[4, 5, 6]), &runtime, &mut output)
            .expect("second shared root batch should execute");
        executor
            .execute_all_into(&integer_chunk(&[7, 8, 9]), &runtime, &mut output)
            .expect("third shared root batch should execute");
        let mut second_allocations = Vec::new();
        output.data[0].collect_allocation_entries(&mut second_allocations);

        // Reusable chunks rotate between two reset buffers. Returning to the
        // first allocation on the third batch proves no retained expression
        // reference forced that buffer through copy-on-write.
        assert_eq!(counter.load(Ordering::SeqCst), 3);
        assert_eq!(first_allocations, second_allocations);
        assert_eq!(output.get_value(0, 0), Some(Value::Integer(7)));
        assert_eq!(output.get_value(0, 2), Some(Value::Integer(9)));
    }

    #[test]
    fn repeated_child_expressions_share_scratch_with_selection() {
        let session = test_session();
        let runtime = test_runtime(session);
        let (shared_expr, counter) = cached_identity_expr(0);
        let greater = Expression::Comparison(ComparisonExpression::new(
            ComparisonType::GreaterThan,
            shared_expr.clone(),
            constant_i32(1),
        ));
        let less = Expression::Comparison(ComparisonExpression::new(
            ComparisonType::LessThan,
            shared_expr,
            constant_i32(4),
        ));
        let mut executor = ExpressionExecutor::with_expressions(&[greater, less]);
        let input = integer_chunk(&[0, 2, 5]);
        let selection = paro_common::test_utils::test_selection(vec![1, 2]);
        let mut output = Chunk::try_new(paro_common::test_utils::test_allocator())
            .expect("test chunk allocation failed");

        executor
            .execute_all_kernel(
                VectorKernelInput::from_chunk(&input)
                    .with_selection(Some(&selection))
                    .with_count(selection.len()),
                &runtime,
                &mut output,
            )
            .expect("shared selected child expressions should execute");

        assert_eq!(counter.load(Ordering::SeqCst), 1);
        let greater_result = output.column(0).expect("greater output");
        let less_result = output.column(1).expect("less output");
        assert_eq!(greater_result.get_bool(0), Some(true));
        assert_eq!(greater_result.get_bool(1), Some(true));
        assert_eq!(less_result.get_bool(0), Some(true));
        assert_eq!(less_result.get_bool(1), Some(false));
    }

    #[test]
    fn repeated_child_expressions_share_scratch_inside_single_root() {
        let session = test_session();
        let runtime = test_runtime(session);
        let (shared_expr, counter) = cached_identity_expr(0);
        let expr = Expression::Comparison(ComparisonExpression::new(
            ComparisonType::Equal,
            shared_expr.clone(),
            shared_expr,
        ));
        let mut executor = ExpressionExecutor::new(&expr);
        let input = integer_chunk(&[7, 8, 9]);

        let result = executor
            .execute_expression(0, &input, None, input.size(), &runtime)
            .expect("shared single-root expression should execute");

        assert_eq!(executor.physical_program().shared_expression_count(), 1);
        assert_eq!(counter.load(Ordering::SeqCst), 1);
        assert_eq!(result.get_bool(0), Some(true));
        assert_eq!(result.get_bool(1), Some(true));
        assert_eq!(result.get_bool(2), Some(true));
    }

    #[test]
    fn expression_program_cache_keys_include_version() {
        let expr = greater_than_i32(0, 10);
        let mut cache = crate::expression_executor::physical::ExpressionProgramCache::default();
        let version = ExpressionProgramVersion::anonymous();

        let first = cache.get_or_compile(std::slice::from_ref(&expr), version.clone());
        let second = cache.get_or_compile(std::slice::from_ref(&expr), version.clone());
        assert!(Arc::ptr_eq(&first, &second));
        assert_eq!(cache.len(), 1);
        assert_eq!(cache.hits(), 1);
        assert_eq!(cache.misses(), 1);

        let mut changed = version;
        changed.settings_fingerprint = 42;
        let third = cache.get_or_compile(std::slice::from_ref(&expr), changed);
        assert!(!Arc::ptr_eq(&first, &third));
        assert_eq!(cache.len(), 2);
    }

    #[test]
    fn expression_program_cache_evicts_lru_without_clearing_all_entries() {
        let keep = greater_than_i32(0, 10);
        let evict = less_than_i32(0, 20);
        let insert = greater_than_i32(0, 30);
        let version = ExpressionProgramVersion::anonymous();
        let mut cache =
            crate::expression_executor::physical::ExpressionProgramCache::with_capacity_limit(2);

        let keep_program = cache.get_or_compile(std::slice::from_ref(&keep), version.clone());
        cache.get_or_compile(std::slice::from_ref(&evict), version.clone());
        let keep_again = cache.get_or_compile(std::slice::from_ref(&keep), version.clone());
        assert!(Arc::ptr_eq(&keep_program, &keep_again));
        cache.get_or_compile(std::slice::from_ref(&insert), version.clone());

        assert_eq!(cache.len(), 2);
        assert!(cache.contains_program(std::slice::from_ref(&keep), &version));
        assert!(cache.contains_program(std::slice::from_ref(&insert), &version));
        assert!(!cache.contains_program(std::slice::from_ref(&evict), &version));
    }

    #[test]
    fn expression_program_cache_rejects_identities_larger_than_its_node_budget() {
        let expr = greater_than_i32(0, 10);
        let version = ExpressionProgramVersion::anonymous();
        let mut cache =
            crate::expression_executor::physical::ExpressionProgramCache::with_limits(16, 1);

        let first = cache.get_or_compile(std::slice::from_ref(&expr), version.clone());
        let second = cache.get_or_compile(std::slice::from_ref(&expr), version);

        assert!(!Arc::ptr_eq(&first, &second));
        assert_eq!(cache.len(), 0);
        assert_eq!(cache.hits(), 0);
        assert_eq!(cache.misses(), 2);
    }

    #[test]
    fn fused_output_set_tracks_wide_projections_without_a_fixed_bit_limit() {
        let mut outputs = FusedOutputSet::new(130);

        assert!(outputs.pair_is_available(64, 129));
        outputs.mark_pair(64, 129);

        assert!(outputs.contains(64));
        assert!(outputs.contains(129));
        assert!(!outputs.pair_is_available(0, 64));
    }

    #[test]
    fn expression_executor_reuses_cached_physical_programs() {
        let expr = greater_than_i32(0, 10);
        let first = ExpressionExecutor::new(&expr);
        let second = ExpressionExecutor::new(&expr);

        assert!(std::ptr::addr_eq(
            first.physical_program(),
            second.physical_program()
        ));
    }

    #[test]
    fn execute_all_into_preserves_projection_schema_for_empty_physical_input() {
        let session = test_session();
        let runtime = test_runtime(session);
        let expressions = [
            Expression::Reference(ReferenceExpression::new(0, LogicalType::Integer)),
            Expression::Reference(ReferenceExpression::new(1, LogicalType::Varchar)),
        ];
        let mut executor = ExpressionExecutor::with_expressions(&expressions);
        let input = paro_common::test_utils::test_empty_chunk(&[]);
        let mut output = Chunk::try_new(paro_common::test_utils::test_allocator())
            .expect("test chunk allocation failed");

        executor
            .execute_all_into(&input, &runtime, &mut output)
            .expect("empty projection should preserve logical output schema");

        assert_eq!(output.size(), 0);
        assert_eq!(output.column_count(), 2);
        assert_eq!(
            output.types(),
            vec![LogicalType::Integer, LogicalType::Varchar]
        );
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
