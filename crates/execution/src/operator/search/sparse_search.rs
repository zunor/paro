// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Physical sparse vector scan operator backed by a storage-owned search cursor.

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

use paro_storage::index::PredicateTree;
use paro_storage::rowset::SparseVector;
use paro_storage::table::table_handle::TableHandle;

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
        ctx.check_cancelled()?;
        let visible_version = i64::try_from(ctx.transaction_visible_version()).unwrap_or(i64::MAX);
        let opened = self.bind_data.table_data.open_sparse_vector_search_cursor(
            self.bind_data.sparse_column_id,
            &self.bind_data.query_vector,
            self.bind_data.k,
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
