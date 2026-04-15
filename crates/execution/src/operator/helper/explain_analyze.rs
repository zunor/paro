//! Physical EXPLAIN ANALYZE operator (sink drains the child; source returns text lines).

use std::any::Any;
use std::sync::Arc;
use std::time::Instant;

use parking_lot::Mutex;
use paro_common::allocator::MemoryTag;
use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use paro_common::vector::VECTOR_SIZE;
use paro_planner::operator::{ExplainFormat, ExplainSpec};

use crate::execution_context::ExecutionContext;
use crate::explain::explain_node::{
    build_explain_doc, render_explain_json_string, render_explain_text_lines,
};
use crate::explain::types::{ExplainProperty, ExplainValue};
use crate::operator::state::{
    GlobalSinkState, GlobalSourceState, LocalSinkState, LocalSourceState, OperatorSinkCombineInput,
    OperatorSinkFinalizeInput, OperatorSinkInput, OperatorSourceInput,
};
use crate::operator::PhysicalOperator;
use crate::operator_type::PhysicalOperatorType;
use crate::result_type::{
    SinkCombineResultType, SinkFinalizeType, SinkResultType, SourceResultType,
};

/// ExplainAnalyze executes child plan then returns analyzed text lines.
#[derive(Debug)]
pub struct ExplainAnalyze {
    output_types: Vec<LogicalType>,
    child: Arc<dyn PhysicalOperator>,
    spec: ExplainSpec,
    sink_state: Mutex<Option<Arc<dyn GlobalSinkState>>>,
}

impl ExplainAnalyze {
    pub fn new(child: Arc<dyn PhysicalOperator>, spec: ExplainSpec) -> Self {
        Self {
            output_types: vec![LogicalType::Varchar],
            child,
            spec,
            sink_state: Mutex::new(None),
        }
    }

    fn render_analyzed_lines(
        &self,
        rows_returned: usize,
        elapsed_ms: f64,
        temp_storage_bytes: u64,
    ) -> Vec<String> {
        let mut doc = build_explain_doc(self.child.as_ref(), self.spec);
        if self.spec.detail.summary {
            if self.spec.detail.timing {
                doc.summary.push(ExplainProperty::new(
                    "Execution Time",
                    ExplainValue::String(format!("{elapsed_ms:.3} ms")),
                ));
            }
            doc.summary.push(ExplainProperty::new(
                "Rows Returned",
                ExplainValue::Unsigned(rows_returned as u64),
            ));
            if self.spec.detail.memory && temp_storage_bytes > 0 {
                doc.summary.push(ExplainProperty::new(
                    "Total Temp Storage",
                    ExplainValue::Bytes(temp_storage_bytes),
                ));
            }
        }
        match self.spec.format {
            ExplainFormat::Text => render_explain_text_lines(&doc),
            ExplainFormat::Json => vec![render_explain_json_string(&doc)],
        }
    }
}

#[derive(Debug)]
struct ExplainAnalyzeGlobalSinkState {
    start_time: Instant,
    rows_returned: Mutex<usize>,
    analyzed_lines: Mutex<Vec<String>>,
    finalized: Mutex<bool>,
}

impl GlobalSinkState for ExplainAnalyzeGlobalSinkState {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn sink_state_name(&self) -> &str {
        "ExplainAnalyzeGlobalSinkState"
    }
}

#[derive(Debug, Default)]
struct ExplainAnalyzeLocalSinkState {
    rows_seen: usize,
}

impl LocalSinkState for ExplainAnalyzeLocalSinkState {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

#[derive(Debug)]
struct ExplainAnalyzeGlobalSourceState {
    analyzed_lines: Vec<String>,
}

impl GlobalSourceState for ExplainAnalyzeGlobalSourceState {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

#[derive(Debug, Default)]
struct ExplainAnalyzeLocalSourceState {
    next_line: usize,
}

impl LocalSourceState for ExplainAnalyzeLocalSourceState {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

impl PhysicalOperator for ExplainAnalyze {
    fn operator_type(&self) -> PhysicalOperatorType {
        PhysicalOperatorType::ExplainAnalyze
    }

    fn types(&self) -> &[LogicalType] {
        &self.output_types
    }

    fn children_count(&self) -> usize {
        1
    }

    fn child(&self, index: usize) -> Option<&dyn PhysicalOperator> {
        if index == 0 {
            Some(self.child.as_ref())
        } else {
            None
        }
    }

    fn child_arc(&self, index: usize) -> Option<Arc<dyn PhysicalOperator>> {
        if index == 0 {
            Some(self.child.clone())
        } else {
            None
        }
    }

    fn set_sink_state(&self, state: Arc<dyn GlobalSinkState>) {
        *self.sink_state.lock() = Some(state);
    }

    fn sink_state(&self) -> Option<Arc<dyn GlobalSinkState>> {
        self.sink_state.lock().clone()
    }

    fn is_sink(&self) -> bool {
        true
    }

    fn parallel_sink(&self) -> bool {
        true
    }

    fn get_global_sink_state(&self, _ctx: &ExecutionContext) -> Result<Box<dyn GlobalSinkState>> {
        Ok(Box::new(ExplainAnalyzeGlobalSinkState {
            start_time: Instant::now(),
            rows_returned: Mutex::new(0),
            analyzed_lines: Mutex::new(Vec::new()),
            finalized: Mutex::new(false),
        }))
    }

    fn get_local_sink_state(&self, _ctx: &ExecutionContext) -> Result<Box<dyn LocalSinkState>> {
        Ok(Box::new(ExplainAnalyzeLocalSinkState::default()))
    }

    fn sink(
        &self,
        _ctx: &ExecutionContext,
        chunk: &Chunk,
        input: &mut OperatorSinkInput,
    ) -> Result<SinkResultType> {
        let lstate = input
            .local_state
            .as_any_mut()
            .downcast_mut::<ExplainAnalyzeLocalSinkState>()
            .ok_or_else(|| {
                paro_error::internal("Invalid local sink state for ExplainAnalyze".to_string())
            })?;
        lstate.rows_seen += chunk.size();
        Ok(SinkResultType::NeedMoreInput)
    }

    fn combine(
        &self,
        _ctx: &ExecutionContext,
        input: &mut OperatorSinkCombineInput,
    ) -> Result<SinkCombineResultType> {
        let gstate = input
            .global_state
            .as_any()
            .downcast_ref::<ExplainAnalyzeGlobalSinkState>()
            .ok_or_else(|| {
                paro_error::internal("Invalid global sink state for ExplainAnalyze".to_string())
            })?;
        let lstate = input
            .local_state
            .as_any_mut()
            .downcast_mut::<ExplainAnalyzeLocalSinkState>()
            .ok_or_else(|| {
                paro_error::internal("Invalid local sink state for ExplainAnalyze".to_string())
            })?;

        *gstate.rows_returned.lock() += lstate.rows_seen;
        lstate.rows_seen = 0;
        Ok(SinkCombineResultType::Finished)
    }

    fn finalize(&self, input: &OperatorSinkFinalizeInput) -> Result<SinkFinalizeType> {
        let gstate = input
            .global_state
            .as_any()
            .downcast_ref::<ExplainAnalyzeGlobalSinkState>()
            .ok_or_else(|| {
                paro_error::internal("Invalid global sink state for ExplainAnalyze".to_string())
            })?;

        let mut finalized = gstate.finalized.lock();
        if *finalized {
            return Ok(SinkFinalizeType::Ready);
        }

        let rows_returned = *gstate.rows_returned.lock();
        let elapsed_ms = gstate.start_time.elapsed().as_secs_f64() * 1000.0;
        let lines = self.render_analyzed_lines(rows_returned, elapsed_ms, 0);
        *gstate.analyzed_lines.lock() = lines;
        *finalized = true;

        Ok(SinkFinalizeType::Ready)
    }

    fn is_source(&self) -> bool {
        true
    }

    fn parallel_source(&self) -> bool {
        false
    }

    fn get_global_source_state(
        &self,
        _ctx: &ExecutionContext,
        sink_state: Option<&dyn GlobalSinkState>,
    ) -> Result<Box<dyn GlobalSourceState>> {
        let internal_state = self.sink_state();
        let state = sink_state.or(internal_state.as_deref()).ok_or_else(|| {
            paro_error::internal("ExplainAnalyze requires sink state for source phase".to_string())
        })?;

        let explain_state = state
            .as_any()
            .downcast_ref::<ExplainAnalyzeGlobalSinkState>()
            .ok_or_else(|| {
                paro_error::internal(format!(
                    "Invalid sink state for ExplainAnalyze. Expected ExplainAnalyzeGlobalSinkState, got {}",
                    state.sink_state_name()
                ))
            })?;

        let lines = {
            let existing = explain_state.analyzed_lines.lock().clone();
            if !existing.is_empty() {
                existing
            } else {
                let rows_returned = *explain_state.rows_returned.lock();
                let elapsed_ms = explain_state.start_time.elapsed().as_secs_f64() * 1000.0;
                self.render_analyzed_lines(rows_returned, elapsed_ms, 0)
            }
        };

        Ok(Box::new(ExplainAnalyzeGlobalSourceState {
            analyzed_lines: lines,
        }))
    }

    fn get_local_source_state(
        &self,
        _ctx: &ExecutionContext,
        _gstate: &dyn GlobalSourceState,
    ) -> Result<Box<dyn LocalSourceState>> {
        Ok(Box::new(ExplainAnalyzeLocalSourceState::default()))
    }

    fn get_data(
        &self,
        ctx: &ExecutionContext,
        chunk: &mut Chunk,
        input: &mut OperatorSourceInput,
    ) -> Result<SourceResultType> {
        let gstate = input
            .global_state
            .as_any()
            .downcast_ref::<ExplainAnalyzeGlobalSourceState>()
            .ok_or_else(|| {
                paro_error::internal("Invalid global source state for ExplainAnalyze".to_string())
            })?;
        let lstate = input
            .local_state
            .as_any_mut()
            .downcast_mut::<ExplainAnalyzeLocalSourceState>()
            .ok_or_else(|| {
                paro_error::internal("Invalid local source state for ExplainAnalyze".to_string())
            })?;

        if lstate.next_line >= gstate.analyzed_lines.len() {
            return Ok(SourceResultType::Finished);
        }

        let remaining = gstate.analyzed_lines.len() - lstate.next_line;
        let output_count = remaining.min(VECTOR_SIZE);
        let allocator = ctx.allocator(MemoryTag::Allocator);
        let mut output_chunk =
            Chunk::initialize_with_allocator(&self.output_types, output_count, allocator);

        let output_vector = output_chunk.column_mut(0).ok_or_else(|| {
            paro_error::internal("ExplainAnalyze output chunk missing column".to_string())
        })?;

        for row_idx in 0..output_count {
            output_vector.set_value(
                row_idx,
                &Value::Varchar(gstate.analyzed_lines[lstate.next_line + row_idx].clone()),
            );
        }
        output_chunk.set_cardinality(output_count);
        lstate.next_line += output_count;
        *chunk = output_chunk;

        Ok(SourceResultType::HaveMoreOutput)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}
