// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Vectorized residual predicates for hash-chain candidates.

use std::sync::Arc;

use paro_common::allocator::MemoryTag;
use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::types::{LogicalType, PhysicalType};
use paro_common::vector::{SelectionVector, Vector};
use paro_function::scalar::FunctionExecContext;
use paro_planner::expression::{
    ComparisonExpression, ComparisonType, ConjunctionExpression, ConjunctionType, Expression,
    ReferenceExpression,
};
use paro_planner::operator::join::{JoinComparisonType, JoinCondition};

use crate::expression_executor::executor::ExpressionExecutor;
use crate::join_hashtable::JoinHashTable;
use crate::operators::join::hash::keys::{evaluate_join_keys_into, join_key_types, JoinKeySide};
use crate::runtime::context::{OperatorCallContext, QueryRuntimeContext};

#[derive(Debug)]
pub struct HashJoinResidualProbeState {
    conditions: Box<[JoinCondition]>,
    left_types: Box<[LogicalType]>,
    left_executors: Box<[ExpressionExecutor]>,
    left_values: Option<Chunk>,
    candidate_types: Box<[LogicalType]>,
    candidates: Option<Chunk>,
    predicate: ResidualPredicate,
    build_residual_offset: usize,
}

#[derive(Debug)]
enum ResidualPredicate {
    I8(JoinComparisonType),
    I16(JoinComparisonType),
    I32(JoinComparisonType),
    I64(JoinComparisonType),
    I128(JoinComparisonType),
    U8(JoinComparisonType),
    U16(JoinComparisonType),
    U32(JoinComparisonType),
    U64(JoinComparisonType),
    U128(JoinComparisonType),
    Generic(ExpressionExecutor),
}

impl HashJoinResidualProbeState {
    pub(crate) fn new(
        conditions: &[JoinCondition],
        session: &paro_context::StatementContext,
    ) -> Option<Self> {
        Self::new_at_offset(conditions, 0, session)
    }

    pub(crate) fn new_at_offset(
        conditions: &[JoinCondition],
        build_residual_offset: usize,
        session: &paro_context::StatementContext,
    ) -> Option<Self> {
        if conditions.is_empty() {
            return None;
        }
        let left_types = join_key_types(conditions, JoinKeySide::Probe);
        let right_types = join_key_types(conditions, JoinKeySide::Build);
        let mut candidate_types = left_types.to_vec();
        candidate_types.extend(right_types.iter().cloned());
        let predicate = compile_residual_predicate(conditions, session);
        Some(Self {
            conditions: conditions.to_vec().into_boxed_slice(),
            left_types,
            left_executors: conditions
                .iter()
                .map(|condition| {
                    ExpressionExecutor::with_expressions_for_session(
                        std::slice::from_ref(&condition.left),
                        session,
                    )
                })
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            left_values: None,
            candidate_types: candidate_types.into_boxed_slice(),
            candidates: None,
            predicate,
            build_residual_offset,
        })
    }

    pub(crate) fn evaluate_probe(
        &mut self,
        ctx: &mut OperatorCallContext,
        input: &Chunk,
    ) -> Result<()> {
        evaluate_join_keys_into(
            ctx,
            input,
            &self.conditions,
            &mut self.left_executors,
            &self.left_types,
            JoinKeySide::Probe,
            &mut self.left_values,
        )
    }

    pub(crate) fn select_matches(
        &mut self,
        runtime: &QueryRuntimeContext,
        hash_table: &JoinHashTable,
        lhs_sel: &SelectionVector,
        rhs_pointers: &[usize],
        match_count: usize,
        output: &mut SelectionVector,
    ) -> Result<usize> {
        let left_values = self
            .left_values
            .as_ref()
            .ok_or_else(|| paro_error::internal("hash join residual probe values missing"))?;
        if rhs_pointers.len() != match_count {
            return Err(paro_error::internal(
                "hash join residual pointer count does not match candidate count",
            ));
        }
        match &mut self.predicate {
            ResidualPredicate::I8(comparison) => {
                return select_fixed_matches::<i8>(
                    left_values,
                    hash_table,
                    *comparison,
                    lhs_sel,
                    rhs_pointers,
                    match_count,
                    output,
                    self.build_residual_offset,
                );
            }
            ResidualPredicate::I16(comparison) => {
                return select_fixed_matches::<i16>(
                    left_values,
                    hash_table,
                    *comparison,
                    lhs_sel,
                    rhs_pointers,
                    match_count,
                    output,
                    self.build_residual_offset,
                );
            }
            ResidualPredicate::I32(comparison) => {
                return select_fixed_matches::<i32>(
                    left_values,
                    hash_table,
                    *comparison,
                    lhs_sel,
                    rhs_pointers,
                    match_count,
                    output,
                    self.build_residual_offset,
                );
            }
            ResidualPredicate::I64(comparison) => {
                return select_fixed_matches::<i64>(
                    left_values,
                    hash_table,
                    *comparison,
                    lhs_sel,
                    rhs_pointers,
                    match_count,
                    output,
                    self.build_residual_offset,
                );
            }
            ResidualPredicate::I128(comparison) => {
                return select_fixed_matches::<i128>(
                    left_values,
                    hash_table,
                    *comparison,
                    lhs_sel,
                    rhs_pointers,
                    match_count,
                    output,
                    self.build_residual_offset,
                );
            }
            ResidualPredicate::U8(comparison) => {
                return select_fixed_matches::<u8>(
                    left_values,
                    hash_table,
                    *comparison,
                    lhs_sel,
                    rhs_pointers,
                    match_count,
                    output,
                    self.build_residual_offset,
                );
            }
            ResidualPredicate::U16(comparison) => {
                return select_fixed_matches::<u16>(
                    left_values,
                    hash_table,
                    *comparison,
                    lhs_sel,
                    rhs_pointers,
                    match_count,
                    output,
                    self.build_residual_offset,
                );
            }
            ResidualPredicate::U32(comparison) => {
                return select_fixed_matches::<u32>(
                    left_values,
                    hash_table,
                    *comparison,
                    lhs_sel,
                    rhs_pointers,
                    match_count,
                    output,
                    self.build_residual_offset,
                );
            }
            ResidualPredicate::U64(comparison) => {
                return select_fixed_matches::<u64>(
                    left_values,
                    hash_table,
                    *comparison,
                    lhs_sel,
                    rhs_pointers,
                    match_count,
                    output,
                    self.build_residual_offset,
                );
            }
            ResidualPredicate::U128(comparison) => {
                return select_fixed_matches::<u128>(
                    left_values,
                    hash_table,
                    *comparison,
                    lhs_sel,
                    rhs_pointers,
                    match_count,
                    output,
                    self.build_residual_offset,
                );
            }
            ResidualPredicate::Generic(_) => {}
        }
        let required_capacity = match_count.max(1);
        let needs_new = self.candidates.as_ref().is_none_or(|candidates| {
            candidates.capacity() < required_capacity
                || candidates.column_count() != self.candidate_types.len()
        });
        if needs_new {
            self.candidates = Some(Chunk::try_initialize(
                &self.candidate_types,
                required_capacity,
                runtime.allocator(MemoryTag::BaseTable),
            )?);
        }
        let candidates = self
            .candidates
            .as_mut()
            .expect("hash join residual candidate chunk initialized");
        candidates.try_reset(runtime.allocator(MemoryTag::BaseTable))?;

        let mut aligned_lhs = lhs_sel.clone();
        aligned_lhs.set_len(match_count);
        for (column_idx, source) in left_values.data.iter().enumerate() {
            candidates.data[column_idx] = Arc::new(Vector::try_dictionary(
                Arc::clone(source),
                aligned_lhs.clone(),
            )?);
        }
        for residual_idx in 0..self.conditions.len() {
            let candidate_idx = self.left_types.len() + residual_idx;
            let output_vector = candidates.column_mut(candidate_idx).ok_or_else(|| {
                paro_error::internal("hash join residual candidate column missing")
            })?;
            // SAFETY: every pointer was obtained from `hash_table` while
            // resolving the current probe chain.
            unsafe {
                hash_table.gather_build_column(
                    rhs_pointers,
                    hash_table.build_output_count() + self.build_residual_offset + residual_idx,
                    output_vector,
                )?
            };
        }
        candidates.try_set_cardinality(match_count)?;
        let ResidualPredicate::Generic(predicate) = &mut self.predicate else {
            return Err(paro_error::internal(
                "fixed residual matcher reached generic evaluation",
            ));
        };
        predicate.select_into(0, candidates, match_count, runtime, output)
    }
}

fn compile_residual_predicate(
    conditions: &[JoinCondition],
    session: &paro_context::StatementContext,
) -> ResidualPredicate {
    if let [condition] = conditions {
        if condition.left.return_type().physical_type()
            == condition.right.return_type().physical_type()
        {
            let comparison = condition.comparison;
            return match condition.left.return_type().physical_type() {
                PhysicalType::Int8 => ResidualPredicate::I8(comparison),
                PhysicalType::Int16 => ResidualPredicate::I16(comparison),
                PhysicalType::Int32 => ResidualPredicate::I32(comparison),
                PhysicalType::Int64 => ResidualPredicate::I64(comparison),
                PhysicalType::Int128 => ResidualPredicate::I128(comparison),
                PhysicalType::UInt8 => ResidualPredicate::U8(comparison),
                PhysicalType::UInt16 => ResidualPredicate::U16(comparison),
                PhysicalType::UInt32 => ResidualPredicate::U32(comparison),
                PhysicalType::UInt64 => ResidualPredicate::U64(comparison),
                PhysicalType::UInt128 => ResidualPredicate::U128(comparison),
                PhysicalType::Bool
                | PhysicalType::Float
                | PhysicalType::Double
                | PhysicalType::Varchar
                | PhysicalType::Bit
                | PhysicalType::List
                | PhysicalType::Struct
                | PhysicalType::Array => {
                    let predicate = residual_predicate(conditions);
                    ResidualPredicate::Generic(ExpressionExecutor::with_expressions_for_session(
                        std::slice::from_ref(&predicate),
                        session,
                    ))
                }
            };
        }
    }
    let predicate = residual_predicate(conditions);
    ResidualPredicate::Generic(ExpressionExecutor::with_expressions_for_session(
        std::slice::from_ref(&predicate),
        session,
    ))
}

fn select_fixed_matches<T>(
    left_values: &Chunk,
    hash_table: &JoinHashTable,
    comparison: JoinComparisonType,
    lhs_sel: &SelectionVector,
    rhs_pointers: &[usize],
    match_count: usize,
    output: &mut SelectionVector,
    build_residual_offset: usize,
) -> Result<usize>
where
    T: Copy + PartialEq + PartialOrd,
{
    let lhs = left_values
        .column(0)
        .ok_or_else(|| paro_error::internal("fixed residual probe column missing"))?;
    output.set_len(match_count);
    let mut accepted = 0usize;
    for match_idx in 0..match_count {
        let lhs_idx = lhs_sel.get(match_idx);
        let lhs_value = if lhs.is_null(lhs_idx) {
            None
        } else {
            // SAFETY: the predicate compiler chose `T` from the vector's
            // physical type and `lhs_idx` came from this probe chunk.
            Some(unsafe { lhs.get_fixed::<T>(lhs_idx) })
        };
        // SAFETY: candidate pointers came from this table's hash chain and the
        // compiler chose `T` from the stored residual column's physical type.
        let rhs_value = unsafe {
            hash_table.read_build_payload_fixed::<T>(
                rhs_pointers[match_idx],
                hash_table.build_output_count() + build_residual_offset,
            )
        };
        if fixed_comparison_matches(lhs_value, rhs_value, comparison) {
            output.set(accepted, match_idx);
            accepted += 1;
        }
    }
    output.set_len(accepted);
    Ok(accepted)
}

#[inline]
fn fixed_comparison_matches<T>(
    left: Option<T>,
    right: Option<T>,
    comparison: JoinComparisonType,
) -> bool
where
    T: PartialEq + PartialOrd,
{
    match comparison {
        JoinComparisonType::DistinctFrom => left != right,
        JoinComparisonType::NotDistinctFrom => left == right,
        JoinComparisonType::Equal => {
            matches!((left, right), (Some(left), Some(right)) if left == right)
        }
        JoinComparisonType::NotEqual => {
            matches!((left, right), (Some(left), Some(right)) if left != right)
        }
        JoinComparisonType::LessThan => {
            matches!((left, right), (Some(left), Some(right)) if left < right)
        }
        JoinComparisonType::LessThanOrEqual => {
            matches!((left, right), (Some(left), Some(right)) if left <= right)
        }
        JoinComparisonType::GreaterThan => {
            matches!((left, right), (Some(left), Some(right)) if left > right)
        }
        JoinComparisonType::GreaterThanOrEqual => {
            matches!((left, right), (Some(left), Some(right)) if left >= right)
        }
    }
}

fn residual_predicate(conditions: &[JoinCondition]) -> Expression {
    let count = conditions.len();
    let mut comparisons = conditions
        .iter()
        .enumerate()
        .map(|(idx, condition)| {
            Expression::Comparison(ComparisonExpression::new(
                comparison_type(condition.comparison),
                Expression::Reference(ReferenceExpression::new(idx, condition.left.return_type())),
                Expression::Reference(ReferenceExpression::new(
                    count + idx,
                    condition.right.return_type(),
                )),
            ))
        })
        .collect::<Vec<_>>();
    if comparisons.len() == 1 {
        comparisons.pop().expect("one residual comparison")
    } else {
        Expression::Conjunction(ConjunctionExpression::new(
            ConjunctionType::And,
            comparisons,
        ))
    }
}

fn comparison_type(comparison: JoinComparisonType) -> ComparisonType {
    match comparison {
        JoinComparisonType::Equal => ComparisonType::Equal,
        JoinComparisonType::NotEqual => ComparisonType::NotEqual,
        JoinComparisonType::LessThan => ComparisonType::LessThan,
        JoinComparisonType::LessThanOrEqual => ComparisonType::LessThanOrEqual,
        JoinComparisonType::GreaterThan => ComparisonType::GreaterThan,
        JoinComparisonType::GreaterThanOrEqual => ComparisonType::GreaterThanOrEqual,
        JoinComparisonType::DistinctFrom => ComparisonType::DistinctFrom,
        JoinComparisonType::NotDistinctFrom => ComparisonType::NotDistinctFrom,
    }
}
