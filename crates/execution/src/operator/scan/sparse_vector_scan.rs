//! Physical sparse vector scan operator.
//!
//! Performs sparse vector search using the sparse index
//! and fetches the resulting rows.

use std::any::Any;
use std::sync::Arc;

use paro_common::allocator::MemoryTag;
use paro_common::chunk::Chunk;
use paro_common::error::Result;
use paro_common::types::LogicalType;

use crate::execution_context::ExecutionContext;
use crate::operator::state::{GlobalSourceState, LocalSourceState, OperatorSourceInput};
use crate::operator::PhysicalOperator;
use crate::operator_type::PhysicalOperatorType;
use crate::result_type::SourceResultType;

use paro_storage::index::PredicateTree;
use paro_storage::rowset::SparseVector;
use paro_storage::table::table_handle::TableHandle;

/// Bind data for sparse vector scan.
#[derive(Debug)]
pub struct SparseVectorScanBindData {
    pub table_data: Arc<TableHandle>,
    pub query_vector: SparseVector,
    pub k: usize,
    pub sparse_column_id: usize,
    pub projected_columns: Vec<usize>,
    pub predicates: Option<PredicateTree>,
}

impl SparseVectorScanBindData {
    pub fn new(
        table_data: Arc<TableHandle>,
        query: SparseVector,
        k: usize,
        sparse_col_idx: usize,
        projected_cols: Vec<usize>,
    ) -> Self {
        Self {
            table_data,
            query_vector: query,
            k,
            sparse_column_id: sparse_col_idx,
            projected_columns: projected_cols,
            predicates: None,
        }
    }

    pub fn with_predicates(mut self, predicates: PredicateTree) -> Self {
        self.predicates = Some(predicates);
        self
    }

    pub fn output_types(&self) -> Vec<LogicalType> {
        let all_types = self.table_data.types();
        self.projected_columns
            .iter()
            .filter_map(|&idx| all_types.get(idx).cloned())
            .collect()
    }
}

/// Global state for sparse vector scan.
#[derive(Debug)]
pub struct SparseVectorScanGlobalState {
    result_chunks: Vec<Chunk>,
    chunks_served: std::sync::atomic::AtomicUsize,
}

impl SparseVectorScanGlobalState {
    fn new() -> Self {
        Self {
            result_chunks: Vec::new(),
            chunks_served: std::sync::atomic::AtomicUsize::new(0),
        }
    }
}

impl GlobalSourceState for SparseVectorScanGlobalState {
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
struct SparseVectorScanLocalState {}

impl LocalSourceState for SparseVectorScanLocalState {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

#[derive(Debug)]
pub struct PhysicalSparseVectorScan {
    output_types: Vec<LogicalType>,
    bind_data: Arc<SparseVectorScanBindData>,
}

impl PhysicalSparseVectorScan {
    pub fn new(bind_data: SparseVectorScanBindData) -> Self {
        let output_types = bind_data.output_types();
        Self {
            output_types,
            bind_data: Arc::new(bind_data),
        }
    }

    fn execute_search(&self) -> Result<Vec<Chunk>> {
        let table = &self.bind_data.table_data;
        let column_id = self.bind_data.sparse_column_id;
        let query = &self.bind_data.query_vector;
        let k = self.bind_data.k;
        let predicate = self.bind_data.predicates.as_ref();
        let projected_columns = &self.bind_data.projected_columns;

        table.sparse_vector_search(column_id, query, k, predicate, projected_columns)
    }
}

impl PhysicalOperator for PhysicalSparseVectorScan {
    fn operator_type(&self) -> PhysicalOperatorType {
        PhysicalOperatorType::SparseVectorScan
    }

    fn types(&self) -> &[LogicalType] {
        &self.output_types
    }

    fn is_source(&self) -> bool {
        true
    }

    fn parallel_source(&self) -> bool {
        false
    }

    fn estimated_cardinality(&self) -> usize {
        self.bind_data.k
    }

    fn get_global_source_state(
        &self,
        ctx: &ExecutionContext,
        _sink_state: Option<&dyn crate::operator::state::GlobalSinkState>,
    ) -> Result<Box<dyn GlobalSourceState>> {
        let mut state = SparseVectorScanGlobalState::new();
        let allocator = ctx.allocator(MemoryTag::VectorIndex);
        state.result_chunks = self
            .execute_search()?
            .into_iter()
            .map(|chunk| chunk.deep_copy_with_allocator(allocator.clone()))
            .collect();
        Ok(Box::new(state))
    }

    fn get_local_source_state(
        &self,
        _ctx: &ExecutionContext,
        _gstate: &dyn GlobalSourceState,
    ) -> Result<Box<dyn LocalSourceState>> {
        Ok(Box::new(SparseVectorScanLocalState::default()))
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
            .downcast_ref::<SparseVectorScanGlobalState>()
            .unwrap();

        let served = gstate
            .chunks_served
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
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
