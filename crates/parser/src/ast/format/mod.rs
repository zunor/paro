// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0
//
// Derived from Databend (https://github.com/datafuselabs/databend),
// Copyright 2021 Datafuse Labs, also licensed under Apache License 2.0.

mod indent_format;
mod pretty_format;

use std::fmt::Display;

#[derive(Clone)]
pub struct FormatTreeNode<T: Display + Clone = String> {
    pub payload: T,
    pub children: Vec<Self>,
}

impl<T> FormatTreeNode<T>
where
    T: Display + Clone,
{
    pub fn new(payload: T) -> Self {
        Self {
            payload,
            children: vec![],
        }
    }

    pub fn with_children(payload: T, children: Vec<Self>) -> Self {
        Self { payload, children }
    }
}
