// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0
//
// Derived from Databend (https://github.com/datafuselabs/databend),
// Copyright 2021 Datafuse Labs, also licensed under Apache License 2.0.

use std::fmt::Display;
use std::fmt::Write;

use super::FormatTreeNode;
use crate::Result;

static INDENT_SIZE: usize = 4;

impl<T> FormatTreeNode<T>
where
    T: Display + Clone,
{
    pub fn format_indent(&self) -> Result<String> {
        let mut buf = String::new();
        self.format_indent_impl(0, &mut buf)?;
        Ok(buf)
    }

    fn format_indent_impl(&self, indent: usize, f: &mut String) -> Result<()> {
        writeln!(f, "{}{}", " ".repeat(indent), &self.payload).unwrap();
        for child in self.children.iter() {
            child.format_indent_impl(indent + INDENT_SIZE, f)?;
        }
        Ok(())
    }
}
