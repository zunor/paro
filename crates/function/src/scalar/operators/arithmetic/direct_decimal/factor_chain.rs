// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Multi-output kernel for a producer-consumer DECIMAL factor chain.

use super::*;

pub fn is_decimal_factor_fusion(function: &BoundScalarFunction) -> bool {
    function
        .bind_data
        .as_deref()
        .and_then(|data| data.as_any().downcast_ref::<DecimalFactorFusionBindData>())
        .is_some()
}

/// Execute two DECIMAL factor functions whose first result is the outer input
/// of the second function in one physical row traversal.
///
/// Both bound functions remain the authority for precision, scale, overflow,
/// and exact wide fallback. Unsupported vector layouts decline before either
/// output is modified.
#[allow(clippy::too_many_arguments)]
pub fn try_execute_decimal_factor_chain(
    first_function: &BoundScalarFunction,
    second_function: &BoundScalarFunction,
    first_outer: &Vector,
    first_inner: &Vector,
    second_inner: &Vector,
    first_result: &mut Vector,
    second_result: &mut Vector,
    count: usize,
) -> Result<bool> {
    let Some(first_plan) = first_function
        .bind_data
        .as_deref()
        .and_then(|data| data.as_any().downcast_ref::<DecimalFactorFusionBindData>())
        .copied()
    else {
        return Ok(false);
    };
    let Some(second_plan) = second_function
        .bind_data
        .as_deref()
        .and_then(|data| data.as_any().downcast_ref::<DecimalFactorFusionBindData>())
        .copied()
    else {
        return Ok(false);
    };
    let first_outer = DecimalInputView::try_new(first_outer, count)?;
    let first_inner = DecimalInputView::try_new(first_inner, count)?;
    let second_inner = DecimalInputView::try_new(second_inner, count)?;
    let first_output = DecimalOutput::try_new(first_result)?;
    let second_output = DecimalOutput::try_new(second_result)?;
    let Some(first_prepared) = PreparedCommonFactor::try_new(first_plan) else {
        return Ok(false);
    };
    let Some(second_prepared) = PreparedCommonFactor::try_new(second_plan) else {
        return Ok(false);
    };
    execute_decimal_factor_chain(
        &first_outer,
        &first_inner,
        &second_inner,
        first_output,
        second_output,
        first_plan,
        second_plan,
        first_prepared,
        second_prepared,
        count,
    )
}

#[derive(Clone, Copy)]
enum PreparedI64Triplet {
    Direct {
        first_outer: *const i64,
        first_inner: *const i64,
        second_inner: *const i64,
    },
    Selected {
        first_outer: *const i64,
        first_inner: *const i64,
        second_inner: *const i64,
        first_outer_selection: *const u32,
        first_inner_selection: *const u32,
        second_inner_selection: *const u32,
    },
}

impl PreparedI64Triplet {
    fn try_new(
        first_outer: &DecimalInputView<'_>,
        first_inner: &DecimalInputView<'_>,
        second_inner: &DecimalInputView<'_>,
    ) -> Option<Self> {
        match (
            &first_outer.access,
            &first_inner.access,
            &second_inner.access,
        ) {
            (
                DecimalInputAccess::DirectI64(first_outer),
                DecimalInputAccess::DirectI64(first_inner),
                DecimalInputAccess::DirectI64(second_inner),
            ) => Some(Self::Direct {
                first_outer: *first_outer,
                first_inner: *first_inner,
                second_inner: *second_inner,
            }),
            (
                DecimalInputAccess::SelectedI64(first_outer),
                DecimalInputAccess::SelectedI64(first_inner),
                DecimalInputAccess::SelectedI64(second_inner),
            ) => {
                let first_outer = DirectMaterializedI64Reader::try_new(first_outer)?;
                let first_inner = DirectMaterializedI64Reader::try_new(first_inner)?;
                let second_inner = DirectMaterializedI64Reader::try_new(second_inner)?;
                Some(Self::Selected {
                    first_outer: first_outer.data,
                    first_inner: first_inner.data,
                    second_inner: second_inner.data,
                    first_outer_selection: first_outer.selection,
                    first_inner_selection: first_inner.selection,
                    second_inner_selection: second_inner.selection,
                })
            }
            _ => None,
        }
    }
}

trait FactorChainInputs: Copy {
    /// # Safety
    ///
    /// `row` must identify a logical row in all three prepared inputs.
    unsafe fn read(self, row: usize) -> (i64, i64, i64);
}

#[derive(Clone, Copy)]
struct DirectFactorChainInputs {
    first_outer: *const i64,
    first_inner: *const i64,
    second_inner: *const i64,
}

impl FactorChainInputs for DirectFactorChainInputs {
    #[inline(always)]
    unsafe fn read(self, row: usize) -> (i64, i64, i64) {
        unsafe {
            (
                *self.first_outer.add(row),
                *self.first_inner.add(row),
                *self.second_inner.add(row),
            )
        }
    }
}

#[derive(Clone, Copy)]
struct SelectedFactorChainInputs {
    first_outer: *const i64,
    first_inner: *const i64,
    second_inner: *const i64,
    first_outer_selection: *const u32,
    first_inner_selection: *const u32,
    second_inner_selection: *const u32,
}

impl FactorChainInputs for SelectedFactorChainInputs {
    #[inline(always)]
    unsafe fn read(self, row: usize) -> (i64, i64, i64) {
        unsafe {
            (
                *self
                    .first_outer
                    .add(*self.first_outer_selection.add(row) as usize),
                *self
                    .first_inner
                    .add(*self.first_inner_selection.add(row) as usize),
                *self
                    .second_inner
                    .add(*self.second_inner_selection.add(row) as usize),
            )
        }
    }
}

pub(super) fn execute_decimal_factor_chain(
    first_outer: &DecimalInputView<'_>,
    first_inner: &DecimalInputView<'_>,
    second_inner: &DecimalInputView<'_>,
    first_output: DecimalOutput,
    second_output: DecimalOutput,
    first_plan: DecimalFactorFusionBindData,
    second_plan: DecimalFactorFusionBindData,
    first_prepared: PreparedCommonFactor,
    second_prepared: PreparedCommonFactor,
    count: usize,
) -> Result<bool> {
    let Some(inputs) = PreparedI64Triplet::try_new(first_outer, first_inner, second_inner) else {
        return Ok(false);
    };
    macro_rules! dispatch_outputs {
        ($inputs:expr) => {
            return match (first_output, second_output) {
                (DecimalOutput::I64(first), DecimalOutput::I64(second)) => {
                    execute_decimal_factor_chain_loop(
                        $inputs,
                        DirectI64Writer(first),
                        DirectI64Writer(second),
                        first_plan,
                        second_plan,
                        first_prepared,
                        second_prepared,
                        count,
                    )
                }
                (DecimalOutput::I64(first), DecimalOutput::I128(second)) => {
                    execute_decimal_factor_chain_loop(
                        $inputs,
                        DirectI64Writer(first),
                        DirectI128Writer(second),
                        first_plan,
                        second_plan,
                        first_prepared,
                        second_prepared,
                        count,
                    )
                }
                (DecimalOutput::I128(first), DecimalOutput::I64(second)) => {
                    execute_decimal_factor_chain_loop(
                        $inputs,
                        DirectI128Writer(first),
                        DirectI64Writer(second),
                        first_plan,
                        second_plan,
                        first_prepared,
                        second_prepared,
                        count,
                    )
                }
                (DecimalOutput::I128(first), DecimalOutput::I128(second)) => {
                    execute_decimal_factor_chain_loop(
                        $inputs,
                        DirectI128Writer(first),
                        DirectI128Writer(second),
                        first_plan,
                        second_plan,
                        first_prepared,
                        second_prepared,
                        count,
                    )
                }
            }
        };
    }
    match inputs {
        PreparedI64Triplet::Direct {
            first_outer,
            first_inner,
            second_inner,
        } => dispatch_outputs!(DirectFactorChainInputs {
            first_outer,
            first_inner,
            second_inner,
        }),
        PreparedI64Triplet::Selected {
            first_outer,
            first_inner,
            second_inner,
            first_outer_selection,
            first_inner_selection,
            second_inner_selection,
        } => dispatch_outputs!(SelectedFactorChainInputs {
            first_outer,
            first_inner,
            second_inner,
            first_outer_selection,
            first_inner_selection,
            second_inner_selection,
        }),
    }
}

#[allow(clippy::too_many_arguments)]
fn execute_decimal_factor_chain_loop<I, W1, W2>(
    inputs: I,
    first_output: W1,
    second_output: W2,
    first_plan: DecimalFactorFusionBindData,
    second_plan: DecimalFactorFusionBindData,
    first_prepared: PreparedCommonFactor,
    second_prepared: PreparedCommonFactor,
    count: usize,
) -> Result<bool>
where
    I: FactorChainInputs,
    W1: DirectDecimalWriter,
    W2: DirectDecimalWriter,
{
    let mut invalid = false;
    for row in 0..count {
        let (first_outer, first_inner, second_inner) = unsafe { inputs.read(row) };
        let (first, first_failed) = first_prepared.evaluate(first_outer, first_inner);
        let (second, second_failed) = second_prepared.evaluate(first, second_inner);
        invalid |= first_failed | second_failed;
        unsafe {
            first_output.write(row, i128::from(first));
            second_output.write(row, i128::from(second));
        }
    }
    if !invalid {
        return Ok(true);
    }

    // The optimistic loop never commits aggregate state. Recompute the whole
    // vector exactly so intermediate precision and wide multiplication retain
    // the same semantics as evaluating both scalar functions independently.
    for row in 0..count {
        let (first_outer, first_inner, second_inner) = unsafe { inputs.read(row) };
        let first = first_plan.evaluate_exact(i128::from(first_outer), i128::from(first_inner))?;
        let second = second_plan.evaluate_exact(first, i128::from(second_inner))?;
        unsafe {
            first_output.write(row, first);
            second_output.write(row, second);
        }
    }
    Ok(true)
}
