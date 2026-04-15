//! Top-N helper operator built on a bounded heap.

use std::any::Any;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use parking_lot::Mutex;
use paro_common::chunk::Chunk;
use paro_common::error::Result;
use paro_common::types::LogicalType;
use paro_planner::expression::Expression;

use crate::execution_context::ExecutionContext;
use crate::explain::explain_node::format_bound_order_by_nodes;
use crate::explain::types::ExplainRuntimeStats;
use crate::expression_executor::executor::ExpressionExecutor;
use crate::operator::state::{
    GlobalSinkState, GlobalSourceState, LocalSinkState, LocalSourceState, OperatorSinkCombineInput,
    OperatorSinkFinalizeInput, OperatorSinkInput, OperatorSourceInput,
};
use crate::operator::PhysicalOperator;
use crate::operator_type::PhysicalOperatorType;
use crate::result_type::{
    SinkCombineResultType, SinkFinalizeType, SinkNextBatchType, SinkResultType, SourceResultType,
};
use paro_planner::binder::ir::OrderByNode;

use super::topn_heap::{TopNBoundaryValue, TopNHeap};

/// Physical TopN operator - combines ORDER BY + LIMIT using a heap.
///
/// This is a blocking operator that maintains a heap of size (limit + offset)
/// to efficiently compute the top N rows without sorting the entire input.
#[derive(Debug)]
pub struct TopN {
    /// Output types
    types: Vec<LogicalType>,
    /// ORDER BY clauses
    orders: Vec<OrderByNode>,
    /// LIMIT value
    limit: usize,
    /// OFFSET value
    offset: usize,
    /// Child operator
    child: Arc<dyn PhysicalOperator>,
    /// Global sink state reused by the source path.
    sink_state: Mutex<Option<Arc<dyn GlobalSinkState>>>,
}

impl TopN {
    /// Create a new TopN operator.
    pub fn new(
        types: Vec<LogicalType>,
        orders: Vec<OrderByNode>,
        limit: usize,
        offset: usize,
        child: Arc<dyn PhysicalOperator>,
    ) -> Self {
        Self {
            types,
            orders,
            limit,
            offset,
            child,
            sink_state: Mutex::new(None),
        }
    }

    /// Get the total number of rows to keep (limit + offset)
    pub fn total_rows(&self) -> usize {
        self.limit.saturating_add(self.offset)
    }
}

/// Global sink state for TopN operator.
///
/// Stores the final merged heap and boundary value for parallel execution.
#[derive(Debug)]
struct TopNGlobalSinkState {
    /// Final merged heap
    heap: Mutex<TopNHeap>,
    /// Boundary value for filtering
    boundary: Arc<TopNBoundaryValue>,
    /// Peak memory observed across local/global TopN states.
    peak_memory_bytes: AtomicUsize,
}

impl TopNGlobalSinkState {
    fn record_peak(&self, bytes: usize) {
        self.peak_memory_bytes.fetch_max(bytes, Ordering::AcqRel);
    }

    fn peak_memory_bytes(&self) -> usize {
        self.peak_memory_bytes.load(Ordering::Acquire)
    }
}

impl GlobalSinkState for TopNGlobalSinkState {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

/// Local sink state for TopN operator.
///
/// Each parallel sink maintains its own local heap and expression state.
struct TopNLocalSinkState {
    /// Local heap for this sink
    heap: TopNHeap,
    /// ORDER BY expressions (cloned for each local state)
    order_expressions: Vec<Expression>,
    /// ORDER BY expression types used to size the scratch chunk
    sort_types: Vec<LogicalType>,
    /// Sort chunk for computed ORDER BY values
    sort_chunk: Chunk,
}

impl std::fmt::Debug for TopNLocalSinkState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TopNLocalSinkState")
            .field("heap", &self.heap)
            .field("order_expressions_count", &self.order_expressions.len())
            .field("sort_types_count", &self.sort_types.len())
            .finish()
    }
}

impl LocalSinkState for TopNLocalSinkState {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

impl TopNLocalSinkState {
    fn prepare_sort_chunk(&mut self, required_capacity: usize) {
        if self.sort_chunk.capacity() < required_capacity {
            self.sort_chunk = Chunk::initialize(&self.sort_types, required_capacity);
        } else {
            self.sort_chunk.reset();
        }
    }

    fn memory_usage_bytes(&self) -> usize {
        self.heap.memory_usage_bytes() + self.sort_chunk.get_allocation_size()
    }
}

/// Global source state for TopN operator.
///
/// Stores the sorted result chunks and manages parallel source execution.
#[derive(Debug)]
struct TopNGlobalSourceState {
    /// Sorted result chunks
    result_chunks: Vec<Chunk>,
    /// Current position in tuples (for batch allocation)
    current_position: Mutex<usize>,
    /// Total number of tuples
    total_tuples: usize,
    /// Next batch index for partition tracking
    next_batch_index: Mutex<usize>,
}

impl GlobalSourceState for TopNGlobalSourceState {
    fn max_threads(&self) -> usize {
        // TUPLES_PER_BATCH = 60 * STANDARD_VECTOR_SIZE
        const CHUNKS_PER_BATCH: usize = 60;
        const TUPLES_PER_BATCH: usize = CHUNKS_PER_BATCH * paro_common::vector::VECTOR_SIZE;

        std::cmp::max(self.total_tuples / TUPLES_PER_BATCH, 1)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

/// Local source state for TopN operator.
///
/// Manages per-thread state for parallel source execution.
#[derive(Debug)]
struct TopNLocalSourceState {
    /// Start position for this thread's batch
    batch_start: usize,
    /// End position for this thread's batch
    batch_end: usize,
    /// Current position within the batch
    current_position: usize,
    /// Batch index for partition tracking
    batch_index: usize,
}

impl LocalSourceState for TopNLocalSourceState {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

impl PhysicalOperator for TopN {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn operator_type(&self) -> PhysicalOperatorType {
        PhysicalOperatorType::TopN
    }

    fn types(&self) -> &[LogicalType] {
        &self.types
    }

    fn explain_params(&self) -> Vec<String> {
        let mut params = Vec::new();
        if !self.orders.is_empty() {
            params.push(format!(
                "Sort Key: {}",
                format_bound_order_by_nodes(&self.orders)
            ));
        }
        params.push(format!("Limit: {}", self.limit));
        if self.offset > 0 {
            params.push(format!("Offset: {}", self.offset));
        }
        params
    }

    fn runtime_memory_stats(&self) -> ExplainRuntimeStats {
        let Some(sink_state) = self.sink_state() else {
            return ExplainRuntimeStats::default();
        };
        let Some(sink_state) = sink_state.as_any().downcast_ref::<TopNGlobalSinkState>() else {
            return ExplainRuntimeStats::default();
        };
        ExplainRuntimeStats {
            spilled: None,
            peak_memory_bytes: Some(sink_state.peak_memory_bytes() as u64),
            temp_storage_bytes: None,
        }
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

    // Sink interface
    fn is_sink(&self) -> bool {
        true
    }

    fn get_global_sink_state(&self, _ctx: &ExecutionContext) -> Result<Box<dyn GlobalSinkState>> {
        let heap = TopNHeap::new(self.types.clone(), &self.orders, self.limit, self.offset);
        let boundary = Arc::new(TopNBoundaryValue::new());

        Ok(Box::new(TopNGlobalSinkState {
            heap: Mutex::new(heap),
            boundary,
            peak_memory_bytes: AtomicUsize::new(0),
        }))
    }

    fn get_local_sink_state(&self, _context: &ExecutionContext) -> Result<Box<dyn LocalSinkState>> {
        let heap = TopNHeap::new(self.types.clone(), &self.orders, self.limit, self.offset);

        // Clone ORDER BY expressions for this local state
        let order_expressions: Vec<Expression> = self
            .orders
            .iter()
            .map(|order| order.expression.clone())
            .collect();

        // Initialize sort chunk with ORDER BY expression types
        let sort_types: Vec<LogicalType> = order_expressions
            .iter()
            .map(|expr| expr.return_type())
            .collect();
        let sort_chunk = Chunk::initialize(&sort_types, paro_common::vector::VECTOR_SIZE);

        Ok(Box::new(TopNLocalSinkState {
            heap,
            order_expressions,
            sort_types,
            sort_chunk,
        }))
    }

    fn sink(
        &self,
        context: &ExecutionContext,
        chunk: &Chunk,
        input: &mut OperatorSinkInput,
    ) -> Result<SinkResultType> {
        let gstate = input
            .global_state
            .as_any()
            .downcast_ref::<TopNGlobalSinkState>()
            .expect("Invalid global sink state type");

        let lstate = input
            .local_state
            .as_any_mut()
            .downcast_mut::<TopNLocalSinkState>()
            .expect("Invalid local sink state type");

        // Evaluate ORDER BY expressions into the scratch sort chunk before heap insertion.
        lstate.prepare_sort_chunk(chunk.size());

        let mut executor = ExpressionExecutor::with_expressions(&lstate.order_expressions);
        executor.execute_all_into(chunk, context, &mut lstate.sort_chunk)?;

        // Sink data into local heap with computed sort values
        lstate
            .heap
            .sink_with_sort_chunk(chunk, &lstate.sort_chunk, Some(&gstate.boundary))?;

        // Reduce heap periodically to prevent memory fragmentation
        // This is called after each sink to keep memory usage under control
        lstate.heap.reduce()?;
        gstate.record_peak(lstate.memory_usage_bytes());

        Ok(SinkResultType::NeedMoreInput)
    }

    fn next_batch(
        &self,
        _ctx: &ExecutionContext,
        _gstate: &dyn GlobalSinkState,
        _lstate: &mut dyn LocalSinkState,
    ) -> Result<SinkNextBatchType> {
        // TopN doesn't need batching - it processes all input at once
        Ok(SinkNextBatchType::Ready)
    }

    fn combine(
        &self,
        _ctx: &ExecutionContext,
        input: &mut OperatorSinkCombineInput,
    ) -> Result<SinkCombineResultType> {
        let gstate = input
            .global_state
            .as_any()
            .downcast_ref::<TopNGlobalSinkState>()
            .expect("Invalid global sink state type");

        let lstate = input
            .local_state
            .as_any_mut()
            .downcast_mut::<TopNLocalSinkState>()
            .expect("Invalid local sink state type");

        // Combine local heap into global heap
        let mut global_heap = gstate.heap.lock();
        global_heap.combine(&mut lstate.heap)?;

        // Reduce after combine to compact memory
        // This is important as combine can significantly increase heap_data size
        global_heap.reduce()?;
        gstate.record_peak(global_heap.memory_usage_bytes() + gstate.boundary.allocation_size());

        Ok(SinkCombineResultType::Finished)
    }

    fn finalize(&self, input: &OperatorSinkFinalizeInput) -> Result<SinkFinalizeType> {
        let gstate = input
            .global_state
            .as_any()
            .downcast_ref::<TopNGlobalSinkState>()
            .expect("Invalid global sink state type");

        // Finalize the heap (sort entries)
        let mut heap = gstate.heap.lock();
        heap.finalize();
        gstate.record_peak(heap.memory_usage_bytes() + gstate.boundary.allocation_size());

        Ok(SinkFinalizeType::Ready)
    }

    // Source interface
    fn is_source(&self) -> bool {
        true
    }

    fn get_global_source_state(
        &self,
        _context: &ExecutionContext,
        sink_state: Option<&dyn GlobalSinkState>,
    ) -> Result<Box<dyn GlobalSourceState>> {
        // Use provided state or fall back to stored state
        let stored_state = self.sink_state();
        let s = sink_state
            .or(stored_state.as_deref())
            .ok_or_else(|| paro_common::error::internal("TopN requires sink state"))?;

        let topn_sink_state = s
            .as_any()
            .downcast_ref::<TopNGlobalSinkState>()
            .ok_or_else(|| {
                paro_common::error::internal(format!(
                    "Invalid global sink state type for TopN. Expected TopNGlobalSinkState, got {}",
                    s.sink_state_name()
                ))
            })?;

        // Extract sorted results from heap
        let mut heap = topn_sink_state.heap.lock();
        let result_chunks = heap.extract_results()?;

        // Calculate total tuples
        let total_tuples: usize = result_chunks.iter().map(|c| c.size()).sum::<usize>();

        Ok(Box::new(TopNGlobalSourceState {
            result_chunks,
            current_position: Mutex::new(0),
            total_tuples,
            next_batch_index: Mutex::new(0),
        }))
    }

    fn get_local_source_state(
        &self,
        _context: &ExecutionContext,
        _global_state: &dyn GlobalSourceState,
    ) -> Result<Box<dyn LocalSourceState>> {
        Ok(Box::new(TopNLocalSourceState {
            batch_start: 0,
            batch_end: 0,
            current_position: 0,
            batch_index: 0,
        }))
    }

    fn get_data(
        &self,
        _context: &ExecutionContext,
        chunk: &mut Chunk,
        input: &mut OperatorSourceInput,
    ) -> Result<SourceResultType> {
        const CHUNKS_PER_BATCH: usize = 60;
        const TUPLES_PER_BATCH: usize = CHUNKS_PER_BATCH * paro_common::vector::VECTOR_SIZE;

        let gstate = input
            .global_state
            .as_any()
            .downcast_ref::<TopNGlobalSourceState>()
            .expect("Invalid global source state type");

        let lstate = input
            .local_state
            .as_any_mut()
            .downcast_mut::<TopNLocalSourceState>()
            .expect("Invalid local source state type");

        // Check if we need to obtain a new batch
        if lstate.current_position >= lstate.batch_end {
            // Obtain new scan indices from the global state
            let mut current_pos = gstate.current_position.lock();
            lstate.batch_start = *current_pos;
            *current_pos += TUPLES_PER_BATCH;
            lstate.batch_end = *current_pos;
            lstate.current_position = lstate.batch_start;

            // Assign batch index
            let mut batch_idx = gstate.next_batch_index.lock();
            lstate.batch_index = *batch_idx;
            *batch_idx += 1;

            // Check if we're past the end
            if lstate.batch_start >= gstate.total_tuples {
                return Ok(SourceResultType::Finished);
            }
        }

        // Scan data from result chunks
        let mut output_count = 0;
        let mut output_vectors = Vec::with_capacity(self.types.len());

        // Initialize output vectors
        for col_type in &self.types {
            output_vectors.push(Arc::new(paro_common::vector::Vector::with_capacity(
                col_type.clone(),
                paro_common::vector::VECTOR_SIZE,
            )));
        }

        // Copy data from result chunks
        let mut tuple_idx = lstate.current_position;
        while output_count < paro_common::vector::VECTOR_SIZE
            && tuple_idx < lstate.batch_end
            && tuple_idx < gstate.total_tuples
        {
            // Find which chunk contains this tuple
            let mut cumulative = 0;
            let mut found = false;

            for source_chunk in &gstate.result_chunks {
                let chunk_size = source_chunk.size();
                if tuple_idx < cumulative + chunk_size {
                    // This tuple is in this chunk
                    let local_idx = tuple_idx - cumulative;

                    // Copy all columns
                    for (col_idx, output_vec) in output_vectors.iter_mut().enumerate() {
                        let src_vec = &source_chunk.data[col_idx];
                        let output_vec_mut = Arc::get_mut(output_vec).unwrap();

                        if src_vec.is_null(local_idx) {
                            output_vec_mut.set_null(output_count, true);
                        } else {
                            copy_value(src_vec, local_idx, output_vec_mut, output_count);
                        }
                    }

                    found = true;
                    break;
                }
                cumulative += chunk_size;
            }

            if !found {
                break;
            }

            output_count += 1;
            tuple_idx += 1;
        }

        lstate.current_position = tuple_idx;

        // Set output chunk
        if output_count > 0 {
            for vec in &mut output_vectors {
                Arc::get_mut(vec).unwrap().set_count(output_count);
            }
            *chunk = Chunk::from_arc_vectors(output_vectors);
            chunk.set_cardinality(output_count);
            Ok(SourceResultType::HaveMoreOutput)
        } else {
            Ok(SourceResultType::Finished)
        }
    }
}

impl TopN {
    pub fn orders(&self) -> &[OrderByNode] {
        &self.orders
    }

    pub fn limit(&self) -> usize {
        self.limit
    }

    pub fn offset(&self) -> usize {
        self.offset
    }
}

/// Helper function to copy a single value from source vector to destination vector.
fn copy_value(
    src: &paro_common::vector::Vector,
    src_idx: usize,
    dst: &mut paro_common::vector::Vector,
    dst_idx: usize,
) {
    // Use copy_at which handles all types including Array
    dst.copy_at(dst_idx, src, src_idx);
}

#[cfg(test)]
mod tests {
    use super::*;
    use paro_common::types::LogicalType;

    #[test]
    fn prepare_sort_chunk_grows_beyond_default_vector_size() {
        let sort_types = vec![LogicalType::Integer];
        let mut state = TopNLocalSinkState {
            heap: TopNHeap::new(vec![LogicalType::Integer], &[], 4, 0),
            order_expressions: Vec::new(),
            sort_types: sort_types.clone(),
            sort_chunk: Chunk::initialize(&sort_types, paro_common::vector::VECTOR_SIZE),
        };

        let required_capacity = paro_common::vector::VECTOR_SIZE + 128;
        state.prepare_sort_chunk(required_capacity);

        assert!(state.sort_chunk.capacity() >= required_capacity);
        assert_eq!(state.sort_chunk.column_count(), sort_types.len());
    }
}
