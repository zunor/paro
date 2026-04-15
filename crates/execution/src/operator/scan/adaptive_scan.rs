use std::any::Any;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use paro_common::chunk::Chunk;
use paro_common::error::Result;
use paro_common::types::LogicalType;

use crate::execution_context::ExecutionContext;
use crate::operator::state::{GlobalSourceState, LocalSourceState, OperatorSourceInput};
use crate::operator::PhysicalOperator;
use crate::operator_type::PhysicalOperatorType;
use crate::query_executor::compiled::{CompiledStatement, ResultColumnDesc};
use crate::query_executor::executor::Executor;
use crate::result_type::SourceResultType;

#[derive(Debug, Clone)]
pub struct AdaptiveCandidatePlan {
    pub label: String,
    pub estimated_cost: f64,
    pub plan: Arc<dyn PhysicalOperator>,
}

#[derive(Debug)]
pub struct AdaptiveScanOperator {
    output_types: Vec<LogicalType>,
    output_names: Vec<String>,
    selected_label: String,
    sequential_cost: f64,
    selected_cost: f64,
    plan: Arc<dyn PhysicalOperator>,
}

#[derive(Debug)]
struct AdaptiveScanGlobalState {
    result_chunks: Vec<Chunk>,
    chunks_served: AtomicUsize,
}

impl GlobalSourceState for AdaptiveScanGlobalState {
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
struct AdaptiveScanLocalState;

impl LocalSourceState for AdaptiveScanLocalState {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

impl AdaptiveScanOperator {
    pub fn new(
        sequential_plan: Arc<dyn PhysicalOperator>,
        sequential_cost: f64,
        candidates: Vec<AdaptiveCandidatePlan>,
        output_names: Vec<String>,
    ) -> Self {
        if let Some(candidate) = candidates
            .iter()
            .filter(|candidate| candidate.label.starts_with("fulltext_"))
            .min_by(|left, right| left.estimated_cost.total_cmp(&right.estimated_cost))
        {
            return Self {
                output_types: candidate.plan.types().to_vec(),
                output_names,
                selected_label: candidate.label.clone(),
                sequential_cost,
                selected_cost: candidate.estimated_cost,
                plan: candidate.plan.clone(),
            };
        }

        let mut selected_label = "sequential".to_string();
        let mut selected_cost = sequential_cost;
        let mut selected_plan = sequential_plan;

        for candidate in candidates {
            if candidate.estimated_cost < selected_cost {
                selected_label = candidate.label;
                selected_cost = candidate.estimated_cost;
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

impl PhysicalOperator for AdaptiveScanOperator {
    fn operator_type(&self) -> PhysicalOperatorType {
        PhysicalOperatorType::AdaptiveScan
    }

    fn explain_name(&self) -> String {
        "ADAPTIVE_SCAN".to_string()
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
        let mut handler = executor.execute(compiled)?;

        let mut result_chunks = Vec::new();
        while let Some(chunk) = handler.fetch()? {
            result_chunks.push(chunk.clone());
        }

        Ok(Box::new(AdaptiveScanGlobalState {
            result_chunks,
            chunks_served: AtomicUsize::new(0),
        }))
    }

    fn get_local_source_state(
        &self,
        _ctx: &ExecutionContext,
        _gstate: &dyn GlobalSourceState,
    ) -> Result<Box<dyn LocalSourceState>> {
        Ok(Box::new(AdaptiveScanLocalState))
    }

    fn get_data(
        &self,
        _ctx: &ExecutionContext,
        chunk: &mut Chunk,
        input: &mut OperatorSourceInput,
    ) -> Result<SourceResultType> {
        let gstate = input
            .global_state
            .as_any()
            .downcast_ref::<AdaptiveScanGlobalState>()
            .expect("invalid adaptive scan global state");

        let served = gstate.chunks_served.fetch_add(1, Ordering::SeqCst);
        if served < gstate.result_chunks.len() {
            *chunk = gstate.result_chunks[served].clone();
            Ok(SourceResultType::HaveMoreOutput)
        } else {
            Ok(SourceResultType::Finished)
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}
