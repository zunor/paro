//! Physical Order Operator
//!
//! ## Design Notes
//! - Thin wrapper around the Sort operator; sorting logic lives in `sorting`.
//! - Supports external sort, parallel sink/source, and fixed output order.

use std::any::Any;
use std::sync::{Arc, Mutex};

use paro_common::chunk::Chunk;
use paro_common::error::Result;
use paro_common::types::LogicalType;

use crate::execution_context::ExecutionContext;
use crate::explain::explain_node::format_bound_order_by_nodes;
use crate::explain::types::ExplainRuntimeStats;
use crate::operator::state::{
    GlobalSinkState, GlobalSourceState, LocalSinkState, LocalSourceState, OperatorSinkInput,
    OperatorSourceInput,
};
use crate::operator::{OrderPreservationType, PhysicalOperator};
use crate::operator_type::PhysicalOperatorType;
use crate::result_type::{
    SinkCombineResultType, SinkFinalizeType, SinkResultType, SourceResultType,
};
use crate::sorting::sort::Sort;
use paro_planner::binder::ir::OrderByNode;

/// Physical order operator.
///
/// This is a thin wrapper around the Sort class that implements the PhysicalOperator interface.
/// All sorting logic is delegated to the Sort class.
#[derive(Debug)]
pub struct Order {
    /// Output types
    types: Vec<LogicalType>,
    /// ORDER BY specifications
    _orders: Vec<OrderByNode>,
    /// Projection map (empty = all columns)
    _projections: Vec<usize>,
    /// Whether this is an index sort
    _is_index_sort: bool,
    /// Child operator
    child: Arc<dyn PhysicalOperator>,
    /// The Sort instance that does the actual work
    sort: Arc<Sort>,
    /// Stored global sink state (for source phase)
    sink_state: Mutex<Option<Arc<dyn GlobalSinkState>>>,
}

/// Global sink state for order operator.
///
/// Wraps the Sort class's global sink state.
#[derive(Debug)]
pub struct OrderGlobalSinkState {
    /// The Sort instance
    sort: Arc<Sort>,
    /// Sort's global sink state
    state: Box<dyn GlobalSinkState>,
}

impl GlobalSinkState for OrderGlobalSinkState {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn sink_state_name(&self) -> &str {
        "OrderGlobalSinkState"
    }
}

/// Local sink state for order operator.
///
/// Wraps the Sort class's local sink state.
#[derive(Debug)]
pub struct OrderLocalSinkState {
    /// Sort's local sink state (lazily initialized)
    state: Option<Box<dyn LocalSinkState>>,
}

impl LocalSinkState for OrderLocalSinkState {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

/// Global source state for order operator.
///
/// Wraps the Sort class's global source state.
#[derive(Debug)]
pub struct OrderGlobalSourceState {
    /// The Sort instance
    sort: Arc<Sort>,
    /// Sort's global source state
    state: Box<dyn GlobalSourceState>,
}

impl GlobalSourceState for OrderGlobalSourceState {
    fn max_threads(&self) -> usize {
        self.state.max_threads()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

/// Local source state for order operator.
///
/// Wraps the Sort class's local source state.
#[derive(Debug)]
pub struct OrderLocalSourceState {
    /// Sort's local source state
    state: Box<dyn LocalSourceState>,
}

impl LocalSourceState for OrderLocalSourceState {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

impl Order {
    /// Create a new order operator.
    ///
    /// # Arguments
    /// * `types` - Output types
    /// * `orders` - ORDER BY specifications
    /// * `projections` - Projection map (empty = all columns)
    /// * `child` - Child operator
    /// * `is_index_sort` - Whether this is an index sort
    pub fn new(
        types: Vec<LogicalType>,
        orders: Vec<OrderByNode>,
        projections: Vec<usize>,
        child: Arc<dyn PhysicalOperator>,
        is_index_sort: bool,
    ) -> Result<Self> {
        // Get input types from child
        let input_types = child.types().to_vec();

        // Create the Sort instance
        let sort = Arc::new(Sort::new(
            orders.clone(),
            input_types,
            projections.clone(),
            is_index_sort,
        )?);

        Ok(Self {
            types,
            _orders: orders,
            _projections: projections,
            _is_index_sort: is_index_sort,
            child,
            sort,
            sink_state: Mutex::new(None),
        })
    }

    pub fn orders(&self) -> &[OrderByNode] {
        &self._orders
    }

    pub fn runtime_memory_stats(&self) -> ExplainRuntimeStats {
        let Some(sink_state) = self.sink_state() else {
            return ExplainRuntimeStats::default();
        };
        let Some(order_sink_state) = sink_state.as_any().downcast_ref::<OrderGlobalSinkState>()
        else {
            return ExplainRuntimeStats::default();
        };
        let Some(sort_sink_state) = order_sink_state
            .state
            .as_any()
            .downcast_ref::<crate::sorting::sort::SortGlobalSinkState>()
        else {
            return ExplainRuntimeStats::default();
        };
        ExplainRuntimeStats {
            spilled: Some(sort_sink_state.is_external()),
            peak_memory_bytes: Some(sort_sink_state.peak_reservation() as u64),
            temp_storage_bytes: None,
        }
    }
}

impl PhysicalOperator for Order {
    fn operator_type(&self) -> PhysicalOperatorType {
        PhysicalOperatorType::OrderBy
    }

    fn types(&self) -> &[LogicalType] {
        &self.types
    }

    fn explain_params(&self) -> Vec<String> {
        let mut params = Vec::new();

        if !self._orders.is_empty() {
            params.push(format!(
                "Sort Key: {}",
                format_bound_order_by_nodes(&self._orders)
            ));
        }

        // EXPLAIN ANALYZE runs after sink/source and can surface whether ORDER BY
        // actually entered external mode at runtime.
        if let Some(sink_state) = self.sink_state() {
            if let Some(order_sink_state) =
                sink_state.as_any().downcast_ref::<OrderGlobalSinkState>()
            {
                if let Some(sort_sink_state) = order_sink_state
                    .state
                    .as_any()
                    .downcast_ref::<crate::sorting::sort::SortGlobalSinkState>(
                ) {
                    params.push(format!("External: {}", sort_sink_state.is_external()));
                }
            }
        }

        params
    }

    fn runtime_memory_stats(&self) -> ExplainRuntimeStats {
        Order::runtime_memory_stats(self)
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

    fn is_source(&self) -> bool {
        true
    }

    fn is_sink(&self) -> bool {
        true
    }

    fn parallel_sink(&self) -> bool {
        true
    }

    fn parallel_source(&self) -> bool {
        true
    }

    fn source_order(&self) -> OrderPreservationType {
        OrderPreservationType::FixedOrder
    }

    fn set_sink_state(&self, state: Arc<dyn GlobalSinkState>) {
        let mut lock = self.sink_state.lock().unwrap();
        *lock = Some(state);
    }

    fn sink_state(&self) -> Option<Arc<dyn GlobalSinkState>> {
        self.sink_state.lock().unwrap().clone()
    }

    // ========== Sink Interface ==========

    fn get_global_sink_state(&self, ctx: &ExecutionContext) -> Result<Box<dyn GlobalSinkState>> {
        let state = self.sort.get_global_sink_state(ctx)?;
        Ok(Box::new(OrderGlobalSinkState {
            sort: Arc::clone(&self.sort),
            state,
        }))
    }

    fn get_local_sink_state(&self, _ctx: &ExecutionContext) -> Result<Box<dyn LocalSinkState>> {
        Ok(Box::new(OrderLocalSinkState { state: None }))
    }

    fn sink(
        &self,
        ctx: &ExecutionContext,
        chunk: &Chunk,
        input: &mut OperatorSinkInput,
    ) -> Result<SinkResultType> {
        let gstate = input
            .global_state
            .as_any()
            .downcast_ref::<OrderGlobalSinkState>()
            .expect("Invalid global state type for Order");

        let lstate = input
            .local_state
            .as_any_mut()
            .downcast_mut::<OrderLocalSinkState>()
            .expect("Invalid local state type for Order");

        // Lazily initialize local state
        if lstate.state.is_none() {
            lstate.state = Some(gstate.sort.get_local_sink_state(ctx)?);
        }

        // Delegate to Sort
        gstate.sort.sink(
            ctx,
            chunk,
            gstate.state.as_ref(),
            lstate.state.as_mut().unwrap().as_mut(),
        )
    }

    fn combine(
        &self,
        ctx: &ExecutionContext,
        input: &mut crate::operator::state::OperatorSinkCombineInput,
    ) -> Result<SinkCombineResultType> {
        let gstate = input
            .global_state
            .as_any()
            .downcast_ref::<OrderGlobalSinkState>()
            .expect("Invalid global state type for Order");

        let lstate = input
            .local_state
            .as_any_mut()
            .downcast_mut::<OrderLocalSinkState>()
            .expect("Invalid local state type for Order");

        // If local state was never initialized, nothing to combine
        if lstate.state.is_none() {
            return Ok(SinkCombineResultType::Finished);
        }

        // Delegate to Sort
        gstate.sort.combine(
            ctx,
            gstate.state.as_ref(),
            lstate.state.as_mut().unwrap().as_mut(),
        )
    }

    fn finalize(
        &self,
        input: &crate::operator::state::OperatorSinkFinalizeInput,
    ) -> Result<SinkFinalizeType> {
        let gstate = input
            .global_state
            .as_any()
            .downcast_ref::<OrderGlobalSinkState>()
            .expect("Invalid global state type for Order");

        // Delegate to Sort
        self.sort.finalize(gstate.state.as_ref())
    }

    // ========== Source Interface ==========

    fn get_global_source_state(
        &self,
        ctx: &ExecutionContext,
        sink_state: Option<&dyn GlobalSinkState>,
    ) -> Result<Box<dyn GlobalSourceState>> {
        // Use provided state or fall back to stored state
        if let Some(s) = sink_state {
            let order_sink_state = s
                .as_any()
                .downcast_ref::<OrderGlobalSinkState>()
                .ok_or_else(|| {
                    let type_id = s.as_any().type_id();
                    paro_common::error::internal(
                        format!("Invalid sink state type for Order. Expected OrderGlobalSinkState, got {} with TypeId: {:?}", s.sink_state_name(), type_id),
                    )
                })?;
            let state = self
                .sort
                .get_global_source_state(ctx, order_sink_state.state.as_ref())?;
            return Ok(Box::new(OrderGlobalSourceState {
                sort: Arc::clone(&self.sort),
                state,
            }));
        }

        let order_sink_state_arc = self.sink_state().ok_or_else(|| {
            paro_common::error::internal(
                "Order requires sink_state to create source state".to_string(),
            )
        })?;

        let order_sink_state = order_sink_state_arc
            .as_any()
            .downcast_ref::<OrderGlobalSinkState>()
            .ok_or_else(|| {
                let type_id = order_sink_state_arc.as_any().type_id();
                paro_common::error::internal(format!(
                    "Invalid sink state type for Order (stored). Got TypeId: {:?}",
                    type_id
                ))
            })?;

        let state = self
            .sort
            .get_global_source_state(ctx, order_sink_state.state.as_ref())?;

        Ok(Box::new(OrderGlobalSourceState {
            sort: Arc::clone(&self.sort),
            state,
        }))
    }

    fn get_local_source_state(
        &self,
        ctx: &ExecutionContext,
        gstate: &dyn GlobalSourceState,
    ) -> Result<Box<dyn LocalSourceState>> {
        let order_gstate = gstate
            .as_any()
            .downcast_ref::<OrderGlobalSourceState>()
            .ok_or_else(|| {
                paro_common::error::internal(
                    "Invalid global source state type for Order".to_string(),
                )
            })?;

        let state = order_gstate
            .sort
            .get_local_source_state(ctx, order_gstate.state.as_ref())?;

        Ok(Box::new(OrderLocalSourceState { state }))
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
            .downcast_ref::<OrderGlobalSourceState>()
            .ok_or_else(|| {
                paro_common::error::internal(
                    "Invalid global source state type for Order".to_string(),
                )
            })?;

        let lstate = input
            .local_state
            .as_any_mut()
            .downcast_mut::<OrderLocalSourceState>()
            .ok_or_else(|| {
                paro_common::error::internal(
                    "Invalid local source state type for Order".to_string(),
                )
            })?;

        // Delegate to Sort
        gstate
            .sort
            .get_data(ctx, chunk, gstate.state.as_ref(), lstate.state.as_mut())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}
