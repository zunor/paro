//! Physical vector scan operator.
//!
//! Performs Approximate Nearest Neighbor (ANN) search using HNSW index
//! and fetches the resulting rows.
//!
//! # Key Design
//!
//! 1. Scatter-Gather:
//!    - Search each segment independently using `segment.vector_search()`
//!    - Collect partial results from all segments
//!    - Merge to find global Top-K `(SegmentId, RowId, Score)`
//!
//! 2. Random Access Fetch:
//!    - Group Top-K rows by Segment
//!    - For each segment, use `ColumnIterator::read_by_rowids` to fetch data
//!    - Construct output Chunk
//!

use std::any::Any;
use std::sync::{Arc, OnceLock};

use paro_common::allocator::MemoryTag;
use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::types::LogicalType;

use crate::execution_context::ExecutionContext;
use crate::operator::state::{GlobalSourceState, LocalSourceState, OperatorSourceInput};
use crate::operator::PhysicalOperator;
use crate::operator_type::PhysicalOperatorType;
use crate::result_type::SourceResultType;

use paro_storage::index::hnsw::types::SearchParams;
use paro_storage::index::PredicateTree;
use paro_storage::table::table_handle::TableHandle;

/// Bind data for vector scan

/// Bind data for vector scan
#[derive(Debug)]
pub struct VectorScanBindData {
    pub table_data: Arc<TableHandle>,
    pub query_vector: Vec<f32>,
    pub k: usize,
    pub vector_column_id: usize, // Column index of the vector column
    pub projected_columns: Vec<usize>, // Column indices to output
    pub predicates: Option<PredicateTree>,
    pub search_params: Option<SearchParams>,
}

impl VectorScanBindData {
    pub fn new(
        table_data: Arc<TableHandle>,
        query: Vec<f32>,
        k: usize,
        vector_col_idx: usize,
        projected_cols: Vec<usize>,
    ) -> Self {
        Self {
            table_data,
            query_vector: query,
            k,
            vector_column_id: vector_col_idx,
            projected_columns: projected_cols,
            predicates: None,
            search_params: None,
        }
    }

    pub fn with_predicates(mut self, predicates: PredicateTree) -> Self {
        self.predicates = Some(predicates);
        self
    }

    pub fn with_params(mut self, params: SearchParams) -> Self {
        self.search_params = Some(params);
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

/// Global state for lazy scatter-merge search and result serving.
#[derive(Debug)]
pub struct VectorScanGlobalState {
    search_result: OnceLock<std::result::Result<Vec<Chunk>, String>>,
    chunks_served: std::sync::atomic::AtomicUsize,
    max_threads: usize,
}

impl VectorScanGlobalState {
    fn new(max_threads: usize) -> Self {
        Self {
            search_result: OnceLock::new(),
            chunks_served: std::sync::atomic::AtomicUsize::new(0),
            max_threads: max_threads.max(1),
        }
    }
}

impl GlobalSourceState for VectorScanGlobalState {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
    fn max_threads(&self) -> usize {
        self.max_threads
    }
}

#[derive(Debug, Default)]
struct VectorScanLocalState {}

impl LocalSourceState for VectorScanLocalState {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

#[derive(Debug)]
pub struct PhysicalVectorScan {
    output_types: Vec<LogicalType>,
    bind_data: Arc<VectorScanBindData>,
}

impl PhysicalVectorScan {
    pub fn new(bind_data: VectorScanBindData) -> Self {
        let output_types = bind_data.output_types();
        Self {
            output_types,
            bind_data: Arc::new(bind_data),
        }
    }

    pub fn search_params(&self) -> Option<SearchParams> {
        self.bind_data.search_params
    }

    /// Execute the vector search and fetch data
    fn execute_search(&self) -> Result<Vec<Chunk>> {
        let table = &self.bind_data.table_data;
        let column_id = self.bind_data.vector_column_id;
        let query = &self.bind_data.query_vector;
        let k = self.bind_data.k;
        let default_params = SearchParams::default();
        let search_params = self
            .bind_data
            .search_params
            .as_ref()
            .unwrap_or(&default_params);
        let predicate = self.bind_data.predicates.as_ref();
        let projected_columns = &self.bind_data.projected_columns;

        // Delegate search and fetch to TableHandle
        table.vector_search(
            column_id,
            query,
            k,
            search_params,
            predicate,
            projected_columns,
        )
    }
}

impl PhysicalOperator for PhysicalVectorScan {
    fn operator_type(&self) -> PhysicalOperatorType {
        PhysicalOperatorType::VectorScan
    }

    fn types(&self) -> &[LogicalType] {
        &self.output_types
    }

    fn explain_params(&self) -> Vec<String> {
        let mut params = vec![format!("k={}", self.bind_data.k)];
        if let Some(search_params) = self.search_params() {
            if let Some(ef) = search_params.ef {
                params.push(format!("hnsw_ef={ef}"));
            }
        }
        params
    }

    fn is_source(&self) -> bool {
        true
    }

    fn parallel_source(&self) -> bool {
        true
    }

    fn estimated_cardinality(&self) -> usize {
        self.bind_data.k
    }

    fn get_global_source_state(
        &self,
        ctx: &ExecutionContext,
        _sink_state: Option<&dyn crate::operator::state::GlobalSinkState>,
    ) -> Result<Box<dyn GlobalSourceState>> {
        let visible_version = i64::try_from(ctx.transaction_visible_version()).unwrap_or(i64::MAX);
        let segment_count = self
            .bind_data
            .table_data
            .visible_segment_count(visible_version)?;
        Ok(Box::new(VectorScanGlobalState::new(segment_count)))
    }

    fn get_local_source_state(
        &self,
        _ctx: &ExecutionContext,
        _gstate: &dyn GlobalSourceState,
    ) -> Result<Box<dyn LocalSourceState>> {
        Ok(Box::new(VectorScanLocalState::default()))
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
            .downcast_ref::<VectorScanGlobalState>()
            .unwrap();

        let search_result = gstate.search_result.get_or_init(|| {
            let allocator = ctx.allocator(MemoryTag::VectorIndex);
            self.execute_search()
                .map(|chunks| {
                    chunks
                        .into_iter()
                        .map(|chunk| chunk.deep_copy_with_allocator(allocator.clone()))
                        .collect()
                })
                .map_err(|err| err.to_string())
        });
        let result_chunks = match search_result {
            Ok(chunks) => chunks,
            Err(message) => return Err(paro_error::internal(message.clone())),
        };

        let served = gstate
            .chunks_served
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        if served < result_chunks.len() {
            *chunk = result_chunks[served].clone();
            Ok(SourceResultType::HaveMoreOutput)
        } else {
            // Ensure downstream operators never observe stale chunk state when source is exhausted.
            *chunk = Chunk::init_empty(self.types());
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::thread_context::ThreadContext;
    use paro_common::vector::Vector;
    use paro_context::{test_support::TestStatementContextBuilder, RuntimeLimits};
    use paro_storage::table::table_factory::TableFactory;
    use paro_storage::table::table_handle::TableHandle;

    fn create_storage(types: &[LogicalType]) -> TableHandle {
        TableFactory::default().create_table(types).unwrap()
    }

    fn test_session() -> Arc<paro_context::StatementContext> {
        TestStatementContextBuilder::minimal()
            .with_limits(RuntimeLimits {
                max_threads: 4,
                max_memory: 64 * 1024 * 1024,
                use_temporary_directory: false,
                temporary_directory: String::new(),
                max_temp_directory_size: None,
                force_external: false,
            })
            .with_visible_version(u64::MAX)
            .build()
    }

    #[test]
    fn vector_scan_reports_visible_segment_parallelism() {
        let vector_type = LogicalType::Array(Box::new(LogicalType::Float), 2);
        let table = Arc::new(create_storage(&[LogicalType::Integer, vector_type]));

        // Three appends create three visible rowsets/segments for the current snapshot.
        for batch in 0..3 {
            let id = batch + 1;
            let chunk = Chunk::from_vectors(vec![
                Vector::from_i32(&[id]),
                Vector::from_embeddings(&[vec![id as f32, 0.0_f32]], 2),
            ]);
            table.append(&chunk).expect("append vector chunk");
        }

        let bind = VectorScanBindData::new(table.clone(), vec![1.0_f32, 0.0_f32], 2, 1, vec![0]);
        let op = PhysicalVectorScan::new(bind);
        assert!(op.parallel_source());

        let session = test_session();
        let thread_ctx = ThreadContext::single_threaded();
        let ctx = ExecutionContext::new(session, &thread_ctx, None);
        let expected_threads = table
            .visible_segment_count(i64::MAX)
            .expect("count visible segments")
            .max(1);
        let gstate = op
            .get_global_source_state(&ctx, None)
            .expect("build global source state");

        assert_eq!(gstate.max_threads(), expected_threads);
    }

    #[test]
    fn vector_scan_explain_params_include_hnsw_ef() {
        let vector_type = LogicalType::Array(Box::new(LogicalType::Float), 2);
        let table = Arc::new(create_storage(&[LogicalType::Integer, vector_type]));
        let bind = VectorScanBindData::new(table, vec![1.0_f32, 0.0_f32], 3, 1, vec![0])
            .with_params(SearchParams {
                ef: Some(256),
                acorn: None,
                random_entry_point: None,
            });
        let op = PhysicalVectorScan::new(bind);

        let params = op.explain_params();
        assert!(params.iter().any(|p| p == "k=3"));
        assert!(params.iter().any(|p| p == "hnsw_ef=256"));
    }
}
