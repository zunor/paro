// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::any::Any;
use std::sync::Arc;
use std::sync::Mutex;

use paro_common::chunk::Chunk;
use paro_common::error::Result;
use paro_common::types::LogicalType;
use paro_storage::search::PreferHint;

use crate::execution_context::ExecutionContext;
use crate::operator::state::{GlobalSourceState, LocalSourceState, OperatorSourceInput};
use crate::operator::PhysicalOperator;
use crate::operator_type::PhysicalOperatorType;
use crate::query_executor::compiled::{CompiledStatement, ResultColumnDesc};
use crate::query_executor::executor::Executor;
use crate::query_executor::stream::ResultHandler;
use crate::result_type::SourceResultType;

#[derive(Debug, Clone)]
pub struct AdaptiveSearchCandidatePlan {
    pub label: String,
    pub estimated_cost: f64,
    pub prefer_hint: Option<PreferHint>,
    pub plan: Arc<dyn PhysicalOperator>,
}

#[derive(Debug)]
pub struct AdaptiveSearchOperator {
    output_types: Vec<LogicalType>,
    output_names: Vec<String>,
    selected_label: String,
    sequential_cost: f64,
    selected_cost: f64,
    plan: Arc<dyn PhysicalOperator>,
}

#[derive(Debug)]
struct AdaptiveSearchGlobalState {
    handler: Mutex<ResultHandler>,
}

impl GlobalSourceState for AdaptiveSearchGlobalState {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn max_threads(&self) -> usize {
        1
    }
}

#[derive(Debug, Default)]
struct AdaptiveSearchLocalState;

impl LocalSourceState for AdaptiveSearchLocalState {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

impl AdaptiveSearchOperator {
    pub fn new(
        sequential_plan: Arc<dyn PhysicalOperator>,
        sequential_cost: f64,
        candidates: Vec<AdaptiveSearchCandidatePlan>,
        output_names: Vec<String>,
    ) -> Self {
        let mut selected_label = "sequential".to_string();
        let mut selected_cost = sequential_cost;
        let mut selected_hint = None;
        let mut selected_plan = sequential_plan;

        for candidate in candidates {
            if candidate_beats_selected(
                candidate.estimated_cost,
                candidate.prefer_hint,
                selected_cost,
                selected_hint,
            ) {
                selected_label = candidate.label;
                selected_cost = candidate.estimated_cost;
                selected_hint = candidate.prefer_hint;
                selected_plan = candidate.plan;
            }
        }

        Self {
            output_types: selected_plan.types().to_vec(),
            output_names,
            selected_label,
            sequential_cost,
            selected_cost,
            plan: selected_plan,
        }
    }
}

fn prefer_hint_rank(hint: PreferHint) -> u8 {
    match hint {
        PreferHint::Latency => 3,
        PreferHint::WarmCache => 2,
        PreferHint::Recall => 1,
    }
}

fn candidate_beats_selected(
    candidate_cost: f64,
    candidate_hint: Option<PreferHint>,
    selected_cost: f64,
    selected_hint: Option<PreferHint>,
) -> bool {
    if candidate_cost < selected_cost {
        return true;
    }

    let max_cost = candidate_cost.abs().max(selected_cost.abs()).max(1.0);
    let cost_gap = (candidate_cost - selected_cost).abs() / max_cost;
    if cost_gap > 0.05 {
        return false;
    }

    candidate_hint.map(prefer_hint_rank).unwrap_or(0)
        > selected_hint.map(prefer_hint_rank).unwrap_or(0)
}

impl PhysicalOperator for AdaptiveSearchOperator {
    fn operator_type(&self) -> PhysicalOperatorType {
        PhysicalOperatorType::AdaptiveScan
    }

    fn explain_name(&self) -> String {
        "ADAPTIVE_SEARCH".to_string()
    }

    fn explain_params(&self) -> Vec<String> {
        vec![
            format!("selected={}", self.selected_label),
            format!("selected_cost={:.3}", self.selected_cost),
            format!("sequential_cost={:.3}", self.sequential_cost),
        ]
    }

    fn types(&self) -> &[LogicalType] {
        &self.output_types
    }

    fn estimated_cardinality(&self) -> usize {
        self.plan.estimated_cardinality()
    }

    fn is_source(&self) -> bool {
        true
    }

    fn parallel_source(&self) -> bool {
        false
    }

    fn get_global_source_state(
        &self,
        ctx: &ExecutionContext,
        _sink_state: Option<&dyn crate::operator::state::GlobalSinkState>,
    ) -> Result<Box<dyn GlobalSourceState>> {
        ctx.check_cancelled()?;
        let executor = Executor::new(ctx.session.clone());
        let compiled = CompiledStatement {
            physical_plan: self.plan.clone(),
            result_schema: self
                .output_names
                .iter()
                .cloned()
                .zip(self.output_types.iter().cloned())
                .map(|(name, logical_type)| ResultColumnDesc { name, logical_type })
                .collect(),
            parameter_types: Vec::new(),
        };
        let handler = executor.execute(compiled)?;

        Ok(Box::new(AdaptiveSearchGlobalState {
            handler: Mutex::new(handler),
        }))
    }

    fn get_local_source_state(
        &self,
        _ctx: &ExecutionContext,
        _gstate: &dyn GlobalSourceState,
    ) -> Result<Box<dyn LocalSourceState>> {
        Ok(Box::new(AdaptiveSearchLocalState))
    }

    fn get_data(
        &self,
        ctx: &ExecutionContext,
        chunk: &mut Chunk,
        input: &mut OperatorSourceInput,
    ) -> Result<SourceResultType> {
        ctx.check_cancelled()?;
        let gstate = input
            .global_state
            .as_any()
            .downcast_ref::<AdaptiveSearchGlobalState>()
            .expect("invalid adaptive search global state");

        let mut handler = gstate.handler.lock().unwrap();
        match handler.fetch()? {
            Some(next_chunk) => {
                *chunk = next_chunk.clone();
                Ok(SourceResultType::HaveMoreOutput)
            }
            None => {
                *chunk = Chunk::try_init_empty(self.types(), chunk.allocator().clone())?;
                Ok(SourceResultType::Finished)
            }
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}
