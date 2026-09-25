// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Shared cost equations and machine calibration, independent of search strategy.

pub mod access;
pub mod calibration;
pub(crate) mod join;
pub(crate) mod join_layout;
pub(crate) mod materialization;
pub(crate) mod operator;
pub mod source;

pub(crate) mod region;

pub(crate) mod ranking;
