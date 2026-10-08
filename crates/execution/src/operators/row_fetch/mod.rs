// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

mod state;
mod transform;

pub use state::{
    RowFetchTableBinding, RowFetchTableState, RowFetchTransformGlobal, RowFetchTransformLocal,
};
pub use transform::RowFetchTransformExec;
