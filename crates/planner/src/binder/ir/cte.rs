// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Binder-owned CTE IR and shared bind state.

use std::sync::{Arc, Mutex, OnceLock};

use paro_common::error::{ParoError, Result};
use paro_common::types::LogicalType;
use paro_parser::ast::Query;

use super::query::BoundQuery;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CTEMaterialize {
    Default,
    Materialized,
    NotMaterialized,
}

#[derive(Debug, Clone)]
pub struct CTEBindInfo {
    pub name: String,
    pub aliases: Vec<String>,
    pub query: Box<Query>,
    pub materialized: CTEMaterialize,
    pub cte_index: usize,
    pub recursive: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CTEBindStatus {
    Unbound,
    Binding,
    Bound,
    Failed,
}

#[derive(Debug, Clone)]
pub struct CTERuntimeState {
    pub ref_count: usize,
    pub status: CTEBindStatus,
    pub last_error: Option<Arc<ParoError>>,
}

impl Default for CTERuntimeState {
    fn default() -> Self {
        Self {
            ref_count: 0,
            status: CTEBindStatus::Unbound,
            last_error: None,
        }
    }
}

#[derive(Debug)]
pub struct CTEBindState {
    pub info: CTEBindInfo,
    pub bound: OnceLock<Arc<CTE>>,
    pub runtime: Mutex<CTERuntimeState>,
}

impl CTEBindState {
    pub fn new(info: CTEBindInfo) -> Self {
        Self {
            info,
            bound: OnceLock::new(),
            runtime: Mutex::new(CTERuntimeState::default()),
        }
    }

    pub fn ref_count(&self) -> Result<usize> {
        let state = self.runtime.lock().map_err(|e| {
            paro_common::error::internal(format!("Failed to lock CTE runtime state: {e}"))
        })?;
        Ok(state.ref_count)
    }
}

#[derive(Debug, Clone)]
pub struct CTE {
    pub name: String,
    pub query: BoundQuery,
    pub names: Vec<String>,
    pub types: Vec<LogicalType>,
    pub materialized: CTEMaterialize,
    pub cte_index: usize,
    pub recursive: Option<RecursiveCTE>,
}

#[derive(Debug, Clone)]
pub struct WithCTE {
    pub ctes: Vec<Arc<CTEBindState>>,
    pub child: Box<BoundQuery>,
}

#[derive(Debug, Clone)]
pub struct RecursiveCTE {
    pub union_all: bool,
    pub anchor: BoundQuery,
    pub recursive: BoundQuery,
}
