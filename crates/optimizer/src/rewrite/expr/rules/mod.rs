// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Optimization rules and expression matchers.

pub mod arithmetic;
pub mod comparison;
pub mod conjunction;
mod constant_evaluator;
pub mod constant_folding;
pub mod expression_matcher;
#[cfg(test)]
pub mod function_matcher;
pub mod move_constants;
pub mod rule;
#[cfg(test)]
pub mod set_matcher;
#[cfg(test)]
pub mod type_matcher;
