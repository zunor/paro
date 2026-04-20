// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Physical vector scan operator backed by a storage-owned search cursor.

use std::any::Any;
use std::sync::Arc;

use paro_common::chunk::Chunk;
use paro_common::error::Result;
use paro_common::types::LogicalType;

use crate::execution_context::ExecutionContext;
use crate::operator::search::driver::{
    build_search_batch_config, build_search_resource_budget, search_get_data, SearchOperatorDriver,
    SearchOperatorGlobalState, SearchOperatorLocalState,
};
use crate::operator::state::{GlobalSourceState, LocalSourceState, OperatorSourceInput};
use crate::operator::PhysicalOperator;
use crate::operator_type::PhysicalOperatorType;
use crate::result_type::SourceResultType;

use paro_storage::index::hnsw::types::SearchParams;
use paro_storage::index::PredicateTree;
use paro_storage::table::table_handle::TableHandle;

#[derive(Debug)]
pub struct VectorScanBindData {
    pub table_data: Arc<TableHandle>,
    pub query_vector: Vec<f32>,
    pub k: usize,
    pub vector_column_id: usize,
    pub projected_columns: Vec<usize>,
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
        ctx.check_cancelled()?;
        let visible_version = i64::try_from(ctx.transaction_visible_version()).unwrap_or(i64::MAX);
        let opened = self.bind_data.table_data.open_vector_search_cursor(
            self.bind_data.vector_column_id,
            &self.bind_data.query_vector,
            self.bind_data.k,
            self.bind_data.search_params.unwrap_or_default(),
            self.bind_data.predicates.clone(),
            visible_version,
        )?;
        let batch_config = build_search_batch_config(self.bind_data.k);
        let budget = build_search_resource_budget(ctx, self.bind_data.k);
        let driver = SearchOperatorDriver::new(
            self.bind_data.table_data.clone(),
            opened,
            batch_config,
            budget,
            self.bind_data.projected_columns.clone(),
            false,
        );
        Ok(Box::new(SearchOperatorGlobalState::new(driver)))
    }

    fn get_local_source_state(
        &self,
        _ctx: &ExecutionContext,
        _gstate: &dyn GlobalSourceState,
    ) -> Result<Box<dyn LocalSourceState>> {
        Ok(Box::new(SearchOperatorLocalState))
    }

    fn get_data(
        &self,
        ctx: &ExecutionContext,
        chunk: &mut Chunk,
        input: &mut OperatorSourceInput,
    ) -> Result<SourceResultType> {
        search_get_data(ctx, chunk, input, self.types())
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
    use paro_storage::table::table_factory::TableFactory;
    use paro_storage::table::table_handle::TableHandle;
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn unique_test_root() -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time")
            .as_nanos();
        std::env::temp_dir().join(format!(
            "paro_vector_search_tests_{}_{}",
            std::process::id(),
            nanos
        ))
    }

    fn create_storage(types: &[LogicalType]) -> TableHandle {
        TableFactory::default()
            .with_storage_root(unique_test_root())
            .create_table(types)
            .unwrap()
    }

    #[test]
    fn vector_search_is_streamed_by_internal_search_driver() {
        let vector_type = LogicalType::Array(Box::new(LogicalType::Float), 2);
        let table = Arc::new(create_storage(&[LogicalType::Integer, vector_type]));
        let bind = VectorScanBindData::new(table, vec![1.0_f32, 0.0_f32], 2, 1, vec![0]);
        let op = PhysicalVectorScan::new(bind);

        assert!(!op.parallel_source());
    }

    #[test]
    fn vector_search_explain_params_include_hnsw_ef() {
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
