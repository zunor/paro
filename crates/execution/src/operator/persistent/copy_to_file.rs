//! Physical COPY TO File Operator
//!
//!
//! PhysicalCopyToFile is a Sink + Source operator:
//! - Sink phase: consume Chunk and write to file via CopyFunction callbacks
//! - Source phase: return the number of rows written

use std::any::Any;
use std::fmt;
use std::path::Path;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use crate::execution_context::ExecutionContext;
use crate::operator::state::{
    GlobalSinkState, GlobalSourceState, LocalSinkState, LocalSourceState, OperatorSinkCombineInput,
    OperatorSinkFinalizeInput, OperatorSinkInput, OperatorSourceInput,
};
use crate::operator::PhysicalOperator;
use crate::operator_type::PhysicalOperatorType;
use crate::result_type::{
    SinkCombineResultType, SinkFinalizeType, SinkResultType, SourceResultType,
};
use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use paro_function::copy::{
    CopyFunction, CopyFunctionBindData, CopyToGlobalState, CopyToLocalState,
};

#[derive(Debug, Default)]
struct CopyToSharedState {
    row_count: AtomicU64,
}

fn build_per_thread_output_path(file_path: &str, file_id: usize) -> String {
    let path = Path::new(file_path);
    let parent = path.parent();
    let stem = path.file_stem().and_then(|s| s.to_str()).unwrap_or("copy");
    let ext = path.extension().and_then(|s| s.to_str());

    let filename = match ext {
        Some(ext) if !ext.is_empty() => format!("{stem}_{file_id}.{ext}"),
        _ => format!("{stem}_{file_id}"),
    };

    match parent {
        Some(parent) if !parent.as_os_str().is_empty() => {
            parent.join(filename).to_string_lossy().to_string()
        }
        _ => filename,
    }
}

#[derive(Debug)]
pub struct PhysicalCopyToFile {
    pub copy_function: CopyFunction,
    pub bind_data: Arc<dyn CopyFunctionBindData>,
    pub file_path: String,
    pub per_thread_output: bool,
    pub return_types: Vec<LogicalType>,
    pub child: Arc<dyn PhysicalOperator>,
    shared_state: Arc<CopyToSharedState>,
}

impl PhysicalCopyToFile {
    pub fn new(
        copy_function: CopyFunction,
        bind_data: Arc<dyn CopyFunctionBindData>,
        file_path: String,
        per_thread_output: bool,
        return_types: Vec<LogicalType>,
        child: Arc<dyn PhysicalOperator>,
    ) -> Self {
        Self {
            copy_function,
            bind_data,
            file_path,
            per_thread_output,
            return_types,
            child,
            shared_state: Arc::new(CopyToSharedState::default()),
        }
    }

    pub fn row_count(&self) -> u64 {
        self.shared_state.row_count.load(Ordering::SeqCst)
    }
}

struct CopyToGlobalSinkState {
    per_thread_output: bool,
    shared_state: Arc<CopyToSharedState>,
    global_state: Option<Mutex<Box<dyn CopyToGlobalState>>>,
    next_file_id: AtomicUsize,
}

impl fmt::Debug for CopyToGlobalSinkState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CopyToGlobalSinkState")
            .field("per_thread_output", &self.per_thread_output)
            .field(
                "row_count",
                &self.shared_state.row_count.load(Ordering::SeqCst),
            )
            .finish()
    }
}

impl GlobalSinkState for CopyToGlobalSinkState {
    fn max_threads(&self, source_max_threads: usize) -> usize {
        if self.per_thread_output {
            source_max_threads.max(1)
        } else {
            1
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn sink_state_name(&self) -> &str {
        "CopyToGlobalSinkState"
    }
}

struct CopyToLocalSinkState {
    local_state: Box<dyn CopyToLocalState>,
    thread_global_state: Option<Box<dyn CopyToGlobalState>>,
}

impl fmt::Debug for CopyToLocalSinkState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CopyToLocalSinkState")
            .field(
                "has_thread_global_state",
                &self.thread_global_state.is_some(),
            )
            .finish()
    }
}

impl LocalSinkState for CopyToLocalSinkState {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

#[derive(Debug)]
struct CopyToGlobalSourceState {
    returned: Mutex<bool>,
    shared_state: Arc<CopyToSharedState>,
}

impl GlobalSourceState for CopyToGlobalSourceState {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

#[derive(Debug, Default)]
struct CopyToLocalSourceState;

impl LocalSourceState for CopyToLocalSourceState {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

impl PhysicalOperator for PhysicalCopyToFile {
    fn operator_type(&self) -> PhysicalOperatorType {
        PhysicalOperatorType::CopyToFile
    }

    fn types(&self) -> &[LogicalType] {
        &self.return_types
    }

    fn explain_params(&self) -> Vec<String> {
        vec![
            format!("File: {}", self.file_path),
            format!("PerThreadOutput: {}", self.per_thread_output),
        ]
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn is_source(&self) -> bool {
        true
    }

    fn is_sink(&self) -> bool {
        true
    }

    fn parallel_sink(&self) -> bool {
        self.per_thread_output
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

    // ========== Sink Interface ==========

    fn get_global_sink_state(&self, _ctx: &ExecutionContext) -> Result<Box<dyn GlobalSinkState>> {
        let global_state = if self.per_thread_output {
            None
        } else {
            Some(Mutex::new((self.copy_function.copy_to_initialize_global)(
                &*self.bind_data,
                &self.file_path,
            )?))
        };
        Ok(Box::new(CopyToGlobalSinkState {
            per_thread_output: self.per_thread_output,
            shared_state: self.shared_state.clone(),
            global_state,
            next_file_id: AtomicUsize::new(0),
        }))
    }

    fn get_local_sink_state(&self, _ctx: &ExecutionContext) -> Result<Box<dyn LocalSinkState>> {
        let local_state = (self.copy_function.copy_to_initialize_local)(&*self.bind_data)?;
        Ok(Box::new(CopyToLocalSinkState {
            local_state,
            thread_global_state: None,
        }))
    }

    fn sink(
        &self,
        _ctx: &ExecutionContext,
        chunk: &Chunk,
        input: &mut OperatorSinkInput,
    ) -> Result<SinkResultType> {
        if chunk.is_empty() {
            return Ok(SinkResultType::NeedMoreInput);
        }

        let gstate = input
            .global_state
            .as_any()
            .downcast_ref::<CopyToGlobalSinkState>()
            .ok_or_else(|| paro_error::internal("Invalid COPY TO global sink state".to_string()))?;
        let lstate = input
            .local_state
            .as_any_mut()
            .downcast_mut::<CopyToLocalSinkState>()
            .ok_or_else(|| paro_error::internal("Invalid COPY TO local sink state".to_string()))?;

        if self.per_thread_output {
            if lstate.thread_global_state.is_none() {
                let file_id = gstate.next_file_id.fetch_add(1, Ordering::SeqCst);
                let thread_path = build_per_thread_output_path(&self.file_path, file_id);
                let state =
                    (self.copy_function.copy_to_initialize_global)(&*self.bind_data, &thread_path)?;
                lstate.thread_global_state = Some(state);
            }

            let thread_global_state = lstate.thread_global_state.as_mut().ok_or_else(|| {
                paro_error::internal("Missing COPY TO per-thread sink state".to_string())
            })?;
            (self.copy_function.copy_to_sink)(
                &*self.bind_data,
                &mut **thread_global_state,
                &mut *lstate.local_state,
                chunk,
            )?;
        } else {
            let global_lock = gstate.global_state.as_ref().ok_or_else(|| {
                paro_error::internal("Missing COPY TO global sink state".to_string())
            })?;
            let mut global_state = global_lock
                .lock()
                .map_err(|e| paro_error::internal(e.to_string()))?;

            (self.copy_function.copy_to_sink)(
                &*self.bind_data,
                &mut **global_state,
                &mut *lstate.local_state,
                chunk,
            )?;
        }

        self.shared_state
            .row_count
            .fetch_add(chunk.len() as u64, Ordering::SeqCst);

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
            .downcast_ref::<CopyToGlobalSinkState>()
            .ok_or_else(|| paro_error::internal("Invalid COPY TO global sink state".to_string()))?;
        let lstate = input
            .local_state
            .as_any_mut()
            .downcast_mut::<CopyToLocalSinkState>()
            .ok_or_else(|| paro_error::internal("Invalid COPY TO local sink state".to_string()))?;

        if self.per_thread_output {
            if lstate.thread_global_state.is_none() {
                let file_id = gstate.next_file_id.fetch_add(1, Ordering::SeqCst);
                let thread_path = build_per_thread_output_path(&self.file_path, file_id);
                let state =
                    (self.copy_function.copy_to_initialize_global)(&*self.bind_data, &thread_path)?;
                lstate.thread_global_state = Some(state);
            }

            if let Some(thread_global_state) = lstate.thread_global_state.as_mut() {
                (self.copy_function.copy_to_combine)(
                    &*self.bind_data,
                    &mut **thread_global_state,
                    &mut *lstate.local_state,
                )?;
                (self.copy_function.copy_to_finalize)(
                    &*self.bind_data,
                    &mut **thread_global_state,
                )?;
            }
        } else {
            let global_lock = gstate.global_state.as_ref().ok_or_else(|| {
                paro_error::internal("Missing COPY TO global sink state".to_string())
            })?;
            let mut global_state = global_lock
                .lock()
                .map_err(|e| paro_error::internal(e.to_string()))?;

            (self.copy_function.copy_to_combine)(
                &*self.bind_data,
                &mut **global_state,
                &mut *lstate.local_state,
            )?;
        }

        Ok(SinkCombineResultType::Finished)
    }

    fn finalize(&self, input: &OperatorSinkFinalizeInput) -> Result<SinkFinalizeType> {
        let gstate = input
            .global_state
            .as_any()
            .downcast_ref::<CopyToGlobalSinkState>()
            .ok_or_else(|| paro_error::internal("Invalid COPY TO global sink state".to_string()))?;

        if self.per_thread_output {
            return Ok(SinkFinalizeType::Ready);
        }

        let global_lock = gstate
            .global_state
            .as_ref()
            .ok_or_else(|| paro_error::internal("Missing COPY TO global sink state".to_string()))?;
        let mut global_state = global_lock
            .lock()
            .map_err(|e| paro_error::internal(e.to_string()))?;

        (self.copy_function.copy_to_finalize)(&*self.bind_data, &mut **global_state)?;
        Ok(SinkFinalizeType::Ready)
    }

    // ========== Source Interface ==========

    fn get_global_source_state(
        &self,
        _ctx: &ExecutionContext,
        _sink_state: Option<&dyn GlobalSinkState>,
    ) -> Result<Box<dyn GlobalSourceState>> {
        Ok(Box::new(CopyToGlobalSourceState {
            returned: Mutex::new(false),
            shared_state: self.shared_state.clone(),
        }))
    }

    fn get_local_source_state(
        &self,
        _ctx: &ExecutionContext,
        _gstate: &dyn GlobalSourceState,
    ) -> Result<Box<dyn LocalSourceState>> {
        Ok(Box::new(CopyToLocalSourceState::default()))
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
            .downcast_ref::<CopyToGlobalSourceState>()
            .ok_or_else(|| {
                paro_error::internal("Invalid COPY TO global source state".to_string())
            })?;

        let mut returned = gstate
            .returned
            .lock()
            .map_err(|e| paro_error::internal(e.to_string()))?;
        if *returned {
            return Ok(SourceResultType::Finished);
        }
        *returned = true;

        let row_count = gstate.shared_state.row_count.load(Ordering::SeqCst);

        let col = chunk
            .column_mut(0)
            .ok_or_else(|| paro_error::internal("Output column not found".to_string()))?;
        col.set_value(0, &Value::BigInt(row_count as i64));
        chunk.set_cardinality(1);

        Ok(SourceResultType::Finished)
    }
}
