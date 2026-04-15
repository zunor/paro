// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Optimization rules and expression matchers.

pub mod arithmetic;
pub mod comparison;
pub mod conjunction;
pub mod constant_folding;
pub mod expression_matcher;
pub mod function_matcher;
pub mod move_constants;
pub mod rule;
pub mod set_matcher;
pub mod type_matcher;
