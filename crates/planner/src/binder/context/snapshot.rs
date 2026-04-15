// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::{BindShared, ColumnLookup, ScopeFrame};
use crate::binder::context::Binding;
use crate::binder::ir::CTEBindState;
use paro_common::error::{self as paro_error, Result};
use std::sync::Arc;

/// Immutable scope snapshot captured when a child binder is created.
#[derive(Debug, Clone)]
pub struct BindSnapshot {
    pub shared: Arc<BindShared>,
    frame: ScopeFrame,
    parent: Option<Arc<BindSnapshot>>,
}

impl BindSnapshot {
    pub fn new(
        shared: Arc<BindShared>,
        frame: ScopeFrame,
        parent: Option<Arc<BindSnapshot>>,
    ) -> Self {
        Self {
            shared,
            frame,
            parent,
        }
    }

    pub fn iter_bindings(&self) -> impl Iterator<Item = &Binding> {
        self.frame.bindings()
    }

    pub fn shared(&self) -> &Arc<BindShared> {
        &self.shared
    }

    pub fn find_binding_by_index(&self, index: usize) -> Option<&Binding> {
        self.iter_bindings().find(|binding| binding.index == index)
    }

    pub fn lookup_column(
        &self,
        table_name: Option<&str>,
        column_name: &str,
        depth: usize,
    ) -> Result<Option<ColumnLookup>> {
        let mut found = None;

        for binding in self.iter_bindings() {
            if let Some(target_table) = table_name {
                if binding.alias != target_table {
                    continue;
                }
            }

            if let Some(column_index) = binding.column_names.iter().position(|c| c == column_name) {
                if found.is_some() {
                    return Err(paro_error::catalog(format!(
                        "Ambiguous column name in outer scope: {}",
                        column_name
                    )));
                }

                found = Some(ColumnLookup {
                    alias: binding.alias.clone(),
                    table_index: binding.index,
                    column_index,
                    return_type: binding.column_types[column_index].clone(),
                    depth,
                });
            }
        }

        if found.is_some() {
            return Ok(found);
        }

        match self.parent() {
            Some(parent) => parent.lookup_column(table_name, column_name, depth + 1),
            None => Ok(None),
        }
    }

    pub fn parent(&self) -> Option<&BindSnapshot> {
        self.parent.as_deref()
    }

    pub fn has_parent(&self) -> bool {
        self.parent.is_some()
    }

    pub fn get_cte(&self, name: &str) -> Option<Arc<CTEBindState>> {
        if let Some(info) = self.frame.get_cte(name) {
            return Some(Arc::clone(info));
        }
        self.parent.as_ref().and_then(|parent| parent.get_cte(name))
    }
}
