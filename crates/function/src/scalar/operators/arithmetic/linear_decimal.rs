// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Exact, NULL-strict linear DECIMAL expressions without intermediate vectors.
//!
//! Only total, equal-scale, narrow DECIMAL add/subtract nodes are admitted.
//! Each original bound node is checked before removing its materialization;
//! casts, scale conversion, overflow-capable operations and CSE stay outside.

use super::*;
use crate::scalar::{FunctionSideEffects, FunctionStability};

const MAX_TERMS: usize = 32;

enum Node {
    Input(usize),
    Binary {
        left: usize,
        right: usize,
        subtract: bool,
    },
}

/// A checked execution lowering, not an algebraic rewrite of the logical plan.
#[derive(Default)]
pub struct DecimalLinearBuilder {
    nodes: Vec<(Node, LogicalType)>,
    inputs: Vec<LogicalType>,
    operations: usize,
}

fn narrow_decimal(ty: &LogicalType) -> Option<(u8, u8)> {
    match ty {
        LogicalType::Decimal { precision, scale }
            if (1..=18).contains(precision) && scale <= precision =>
        {
            Some((*precision, *scale))
        }
        _ => None,
    }
}

impl DecimalLinearBuilder {
    pub fn input(&mut self, ty: LogicalType) -> Option<usize> {
        narrow_decimal(&ty)?;
        if self.inputs.len() >= MAX_TERMS {
            return None;
        }
        let id = self.nodes.len();
        self.nodes
            .push((Node::Input(self.inputs.len()), ty.clone()));
        self.inputs.push(ty);
        Some(id)
    }

    pub fn accepts(function: &BoundScalarFunction) -> bool {
        let Some(data) = function
            .bind_data
            .as_deref()
            .and_then(|data| data.as_any().downcast_ref::<DecimalArithmeticBindData>())
        else {
            return false;
        };
        matches!(data.op, DecimalArithmeticOp::Add | DecimalArithmeticOp::Sub)
            && function.error_mode == FunctionErrorMode::Infallible
            && function.stability == FunctionStability::Consistent
            && function.side_effects == FunctionSideEffects::NoSideEffects
            && narrow_decimal(&function.return_type).is_some()
    }

    pub fn binary(
        &mut self,
        function: &BoundScalarFunction,
        left: usize,
        right: usize,
    ) -> Option<usize> {
        if !Self::accepts(function) {
            return None;
        }
        let left_type = &self.nodes.get(left)?.1;
        let right_type = &self.nodes.get(right)?.1;
        if function.arguments.as_slice() != [left_type.clone(), right_type.clone()] {
            return None;
        }
        let (lp, ls) = narrow_decimal(left_type)?;
        let (rp, rs) = narrow_decimal(right_type)?;
        let (precision, scale) = narrow_decimal(&function.return_type)?;
        // Independently establish totality; a stale function flag is not a proof.
        if ls != scale || rs != scale || lp.max(rp) + 1 > precision {
            return None;
        }
        let data = function
            .bind_data
            .as_deref()?
            .as_any()
            .downcast_ref::<DecimalArithmeticBindData>()?;
        let id = self.nodes.len();
        self.nodes.push((
            Node::Binary {
                left,
                right,
                subtract: data.op == DecimalArithmeticOp::Sub,
            },
            function.return_type.clone(),
        ));
        self.operations += 1;
        Some(id)
    }

    pub fn finish(self, root: usize) -> Option<BoundScalarFunction> {
        if self.operations < 2 {
            return None;
        }
        let return_type = self.nodes.get(root)?.1.clone();
        let (precision, _) = narrow_decimal(&return_type)?;
        let mut signs = vec![None; self.inputs.len()];
        let mut stack = vec![(root, false)];
        while let Some((id, negative)) = stack.pop() {
            match self.nodes.get(id)?.0 {
                Node::Input(input) => {
                    // Inputs are occurrence-local. Do not erase a-a's NULL demand
                    // or duplicate an independently shared expression implicitly.
                    if signs[input].replace(negative).is_some() {
                        return None;
                    }
                }
                Node::Binary {
                    left,
                    right,
                    subtract,
                } => {
                    stack.push((right, negative ^ subtract));
                    stack.push((left, negative));
                }
            }
        }
        let signs = signs.into_iter().collect::<Option<Box<[_]>>>()?;
        // The absolute leaf envelope also bounds every reassociated partial sum.
        let envelope = self.inputs.iter().try_fold(0_i64, |sum, ty| {
            let (p, _) = narrow_decimal(ty)?;
            sum.checked_add(10_i64.pow(u32::from(p)) - 1)
        })?;
        if envelope >= 10_i64.pow(u32::from(precision)) {
            return None;
        }
        Some(
            BoundScalarFunction::from(ScalarFunction::new(
                "decimal_linear_fusion".into(),
                self.inputs,
                return_type,
                execute,
            ))
            .with_bind_data(LinearBindData { signs, precision })
            .with_error_mode(FunctionErrorMode::Infallible),
        )
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct LinearBindData {
    signs: Box<[bool]>,
    precision: u8,
}

impl FunctionData for LinearBindData {
    fn clone_box(&self) -> Box<dyn FunctionData> {
        Box::new(self.clone())
    }
    fn equals(&self, other: &dyn FunctionData) -> bool {
        other.as_any().downcast_ref::<Self>() == Some(self)
    }
    fn fingerprint(&self) -> u64 {
        function_data_fingerprint(self)
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
}

fn execute(chunk: &Chunk, state: &dyn ExpressionState, result: &mut Vector) -> Result<()> {
    let plan = state
        .bind_data()
        .and_then(|data| data.as_any().downcast_ref::<LinearBindData>())
        .ok_or_else(|| paro_error::internal("linear decimal contract is missing"))?;
    if chunk.column_count() != plan.signs.len() {
        return Err(paro_error::internal("linear decimal input arity mismatch"));
    }
    let inputs = chunk
        .data
        .iter()
        .map(|v| v.try_decode_ref(chunk.size()))
        .collect::<Result<smallvec::SmallVec<[_; 8]>>>()?;
    result.set_count(chunk.size());
    // All inputs have the exact physical i64 domain certified at construction.
    // Decoded views retain dictionary/constant selection and its NULL mapping.
    let output = unsafe { result.flat_data_mut::<i64>() };
    let limit = 10_i64.pow(u32::from(plan.precision));
    for row in 0..chunk.size() {
        let mut total = 0_i64;
        let mut valid = true;
        for (input, negative) in inputs.iter().zip(plan.signs.iter()) {
            if !input.is_valid(row) {
                valid = false;
                break;
            }
            let value = unsafe { input.get_value::<i64>(row) };
            total = if *negative {
                total.checked_sub(value)
            } else {
                total.checked_add(value)
            }
            .ok_or_else(|| decimal_overflow(DecimalArithmeticOp::Add))?;
        }
        if !valid {
            result.try_set_null(row, true)?;
        } else {
            if total <= -limit || total >= limit {
                return Err(decimal_overflow(DecimalArithmeticOp::Add));
            }
            unsafe {
                *output.add(row) = total;
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use paro_common::test_utils::{test_chunk_from_vectors, test_vector};
    use paro_common::vector::SelectionVector;
    use std::sync::Arc;

    fn money(p: u8, scale: u8) -> LogicalType {
        LogicalType::Decimal {
            precision: p,
            scale,
        }
    }

    fn bind(name: &str, left: &LogicalType, right: &LogicalType) -> BoundScalarFunction {
        let mut set = ScalarFunctionSet::new(name.into());
        register_arithmetic_functions(&mut set);
        let (function, types) = set.bind(&[left.clone(), right.clone()]).unwrap();
        function
            .bind(&ScalarBindInput::new(types, vec![None, None]))
            .unwrap()
    }

    #[test]
    fn nullable_selected_linear_tree_matches_independent_integer_oracle() {
        let ty = money(15, 2);
        let sub = bind("-", &ty, &ty);
        let root = bind("-", &sub.return_type, &sub.return_type);
        let mut builder = DecimalLinearBuilder::default();
        let ids = (0..4)
            .map(|_| builder.input(ty.clone()).unwrap())
            .collect::<Vec<_>>();
        let left = builder.binary(&sub, ids[0], ids[1]).unwrap();
        let right = builder.binary(&sub, ids[2], ids[3]).unwrap();
        let root = builder.binary(&root, left, right).unwrap();
        let fused = builder.finish(root).unwrap();
        let state = super::super::tests::BindState {
            bind_data: fused.bind_data.clone().unwrap(),
        };
        for selected in [false, true] {
            let mut vectors = Vec::new();
            for col in 0..4 {
                let mut vector = test_vector(ty.clone());
                vector.set_count(257);
                for row in 0..257 {
                    let value = (row as i64 * 7919 - col * 997) * if col % 2 == 0 { 1 } else { -1 };
                    vector.set_i64(row, value);
                    if row % (11 + col as usize) == 0 {
                        vector.try_set_null(row, true).unwrap();
                    }
                }
                if selected {
                    let selection = SelectionVector::try_from_indices(
                        (0..257).map(|i| ((i * 37) % 257) as u32).collect(),
                        vector.allocator().clone(),
                    )
                    .unwrap();
                    vector = Vector::try_dictionary(Arc::new(vector), selection).unwrap();
                }
                vectors.push(vector);
            }
            let chunk = test_chunk_from_vectors(vectors);
            let mut result = test_vector(fused.return_type.clone());
            fused.execute(&chunk, &state, &mut result).unwrap();
            for row in 0..257 {
                let values = chunk
                    .data
                    .iter()
                    .map(|v| (!v.is_null(row)).then(|| unsafe { v.get_fixed::<i64>(row) } as i128))
                    .collect::<Option<Vec<_>>>();
                match values {
                    None => assert!(result.is_null(row)),
                    Some(v) => {
                        assert!(!result.is_null(row));
                        assert_eq!(
                            unsafe { result.get_fixed::<i64>(row) } as i128,
                            (v[0] - v[1]) - (v[2] - v[3])
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn linear_constants_empty_input_and_precision_boundary() {
        let ty = money(15, 2);
        let add = bind("+", &ty, &ty);
        let sub = bind("-", &add.return_type, &ty);
        let mut builder = DecimalLinearBuilder::default();
        let a = builder.input(ty.clone()).unwrap();
        let b = builder.input(ty.clone()).unwrap();
        let c = builder.input(ty.clone()).unwrap();
        let first = builder.binary(&add, a, b).unwrap();
        let root = builder.binary(&sub, first, c).unwrap();
        let fused = builder.finish(root).unwrap();
        let state = super::super::tests::BindState {
            bind_data: fused.bind_data.clone().unwrap(),
        };
        for count in [0, 1, 67] {
            for nullable in [false, true] {
                let max = 999_999_999_999_999_i64;
                let constant =
                    |value| paro_common::test_utils::test_constant(ty.clone(), value, count);
                let chunk = test_chunk_from_vectors(vec![
                    constant(max),
                    constant(-max),
                    if nullable {
                        paro_common::test_utils::test_constant_null(ty.clone(), count)
                    } else {
                        constant(max)
                    },
                ]);
                let mut output = test_vector(fused.return_type.clone());
                fused.execute(&chunk, &state, &mut output).unwrap();
                assert_eq!(output.len(), count);
                for row in 0..count {
                    assert_eq!(output.is_null(row), nullable);
                    if !nullable {
                        assert_eq!(unsafe { output.get_fixed::<i64>(row) }, -max);
                    }
                }
            }
        }
    }

    #[test]
    fn rejects_scale_change_overflow_effects_and_shared_occurrence_alias() {
        let narrow = money(15, 2);
        let mut builder = DecimalLinearBuilder::default();
        let left = builder.input(narrow.clone()).unwrap();
        let scaled = builder.input(money(15, 3)).unwrap();
        assert!(builder
            .binary(&bind("+", &narrow, &money(15, 3)), left, scaled)
            .is_none());
        assert!(!DecimalLinearBuilder::accepts(&bind(
            "+",
            &money(38, 2),
            &money(38, 2)
        )));
        assert!(!DecimalLinearBuilder::accepts(&bind("*", &narrow, &narrow)));
        let mut volatile = bind("+", &narrow, &narrow);
        volatile.stability = FunctionStability::Volatile;
        assert!(!DecimalLinearBuilder::accepts(&volatile));
        let first = builder
            .binary(&bind("+", &narrow, &narrow), left, left)
            .unwrap();
        let root = builder
            .binary(&bind("+", &money(16, 2), &narrow), first, left)
            .unwrap();
        assert!(builder.finish(root).is_none());
    }
}
