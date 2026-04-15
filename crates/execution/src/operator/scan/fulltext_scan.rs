// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Physical full-text scan operator.
//!
//! Performs full-text search using the full-text index and fetches the resulting rows.

use std::any::Any;
use std::sync::Arc;

use paro_common::chunk::Chunk;
use paro_common::error::Result;
use paro_common::types::LogicalType;

use crate::execution_context::ExecutionContext;
use crate::operator::state::{GlobalSourceState, LocalSourceState, OperatorSourceInput};
use crate::operator::PhysicalOperator;
use crate::operator_type::PhysicalOperatorType;
use crate::result_type::SourceResultType;

use paro_storage::index::fulltext::query_parser::{
    parse_phraseto_tsquery, parse_plainto_tsquery, parse_query, parse_to_tsquery,
    parse_websearch_to_tsquery, ParsedQuery,
};
use paro_storage::index::fulltext::text_index::GlobalFullTextStats;
use paro_storage::index::fulltext::tokenizer::{tokenizer_from_config, Tokenizer};
use paro_storage::index::PredicateTree;
use paro_storage::table::table_handle::TableHandle;

const SIMPLE_CONFIG: &str = "simple";
const MIN_TOKEN_LEN: usize = 1;
const MAX_TOKEN_LEN: Option<usize> = None;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FullTextQueryKind {
    Legacy,
    TsQuery,
    Plain,
    Phrase,
    WebSearch,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FullTextExecMode {
    Filter,
    ScoreTopK,
}

/// Bind data for full-text scan.
#[derive(Debug)]
pub struct FullTextScanBindData {
    pub table_data: Arc<TableHandle>,
    pub query_text: String,
    pub k: usize,
    pub text_column_id: usize,
    pub projected_columns: Vec<usize>,
    pub predicates: Option<PredicateTree>,
    pub query_kind: FullTextQueryKind,
    pub mode: FullTextExecMode,
    pub config: String,
    pub emit_score: bool,
}

impl FullTextScanBindData {
    pub fn new(
        table_data: Arc<TableHandle>,
        query_text: String,
        k: usize,
        text_column_id: usize,
        projected_columns: Vec<usize>,
    ) -> Self {
        Self {
            table_data,
            query_text,
            k,
            text_column_id,
            projected_columns,
            predicates: None,
            query_kind: FullTextQueryKind::Legacy,
            mode: FullTextExecMode::ScoreTopK,
            config: SIMPLE_CONFIG.to_string(),
            emit_score: false,
        }
    }

    pub fn with_predicates(mut self, predicates: PredicateTree) -> Self {
        self.predicates = Some(predicates);
        self
    }

    pub fn with_query_options(mut self, query_kind: FullTextQueryKind, config: String) -> Self {
        self.query_kind = query_kind;
        self.config = config;
        self
    }

    pub fn with_exec_mode(mut self, mode: FullTextExecMode) -> Self {
        self.mode = mode;
        self
    }

    pub fn with_emit_score(mut self, emit_score: bool) -> Self {
        self.emit_score = emit_score;
        self
    }

    pub fn output_types(&self) -> Vec<LogicalType> {
        let all_types = self.table_data.types();
        let mut output_types: Vec<LogicalType> = self
            .projected_columns
            .iter()
            .filter_map(|&idx| all_types.get(idx).cloned())
            .collect();
        if self.emit_score && matches!(self.mode, FullTextExecMode::ScoreTopK) {
            output_types.push(LogicalType::Float);
        }
        output_types
    }
}

/// Global state for full-text scan.
#[derive(Debug)]
pub struct FullTextScanGlobalState {
    result_chunks: Vec<Chunk>,
    chunks_served: std::sync::atomic::AtomicUsize,
}

impl FullTextScanGlobalState {
    fn new() -> Self {
        Self {
            result_chunks: Vec::new(),
            chunks_served: std::sync::atomic::AtomicUsize::new(0),
        }
    }
}

impl GlobalSourceState for FullTextScanGlobalState {
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
struct FullTextScanLocalState {}

impl LocalSourceState for FullTextScanLocalState {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

#[derive(Debug)]
pub struct PhysicalFullTextScan {
    output_types: Vec<LogicalType>,
    bind_data: Arc<FullTextScanBindData>,
}

impl PhysicalFullTextScan {
    pub fn new(bind_data: FullTextScanBindData) -> Self {
        let output_types = bind_data.output_types();
        Self {
            output_types,
            bind_data: Arc::new(bind_data),
        }
    }

    fn execute_search(&self) -> Result<Vec<Chunk>> {
        let table = &self.bind_data.table_data;
        let column_id = self.bind_data.text_column_id;
        let predicate = self.bind_data.predicates.as_ref();
        let projected_columns = &self.bind_data.projected_columns;
        let parsed_query = self.parse_query()?;

        match self.bind_data.mode {
            FullTextExecMode::Filter => {
                table.fulltext_filter(column_id, &parsed_query, predicate, projected_columns)
            }
            FullTextExecMode::ScoreTopK => {
                let global_stats = self.collect_global_stats();
                let emit_score = self.bind_data.emit_score
                    && matches!(self.bind_data.mode, FullTextExecMode::ScoreTopK);
                table.fulltext_search_parsed(
                    column_id,
                    &parsed_query,
                    self.bind_data.k,
                    predicate,
                    projected_columns,
                    global_stats.as_ref(),
                    emit_score,
                )
            }
        }
    }

    fn collect_global_stats(&self) -> Option<GlobalFullTextStats> {
        let stats = self
            .bind_data
            .table_data
            .fulltext_index_statistics(self.bind_data.text_column_id as u32)?;
        Some(GlobalFullTextStats::from_totals(
            stats.total_docs,
            stats.total_terms,
        ))
    }

    fn parse_query(&self) -> Result<ParsedQuery> {
        let (_kind, tokenizer) = tokenizer_from_config(&self.bind_data.config)?;
        let query = self.bind_data.query_text.as_str();
        self.parse_query_with_tokenizer(query, tokenizer.as_ref())
    }

    fn parse_query_with_tokenizer(
        &self,
        query: &str,
        tokenizer: &dyn Tokenizer,
    ) -> Result<ParsedQuery> {
        match self.bind_data.query_kind {
            FullTextQueryKind::Legacy => {
                parse_query(query, tokenizer, MIN_TOKEN_LEN, MAX_TOKEN_LEN)
            }
            FullTextQueryKind::TsQuery => {
                parse_to_tsquery(query, tokenizer, MIN_TOKEN_LEN, MAX_TOKEN_LEN)
            }
            FullTextQueryKind::Plain => {
                parse_plainto_tsquery(query, tokenizer, MIN_TOKEN_LEN, MAX_TOKEN_LEN)
            }
            FullTextQueryKind::Phrase => {
                parse_phraseto_tsquery(query, tokenizer, MIN_TOKEN_LEN, MAX_TOKEN_LEN)
            }
            FullTextQueryKind::WebSearch => {
                parse_websearch_to_tsquery(query, tokenizer, MIN_TOKEN_LEN, MAX_TOKEN_LEN)
            }
        }
    }
}

impl PhysicalOperator for PhysicalFullTextScan {
    fn operator_type(&self) -> PhysicalOperatorType {
        PhysicalOperatorType::FullTextScan
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
        match self.bind_data.mode {
            FullTextExecMode::Filter => self.bind_data.table_data.total_rows(),
            FullTextExecMode::ScoreTopK => self.bind_data.k,
        }
    }

    fn get_global_source_state(
        &self,
        _ctx: &ExecutionContext,
        _sink_state: Option<&dyn crate::operator::state::GlobalSinkState>,
    ) -> Result<Box<dyn GlobalSourceState>> {
        let mut state = FullTextScanGlobalState::new();
        state.result_chunks = self.execute_search()?;
        Ok(Box::new(state))
    }

    fn get_local_source_state(
        &self,
        _ctx: &ExecutionContext,
        _gstate: &dyn GlobalSourceState,
    ) -> Result<Box<dyn LocalSourceState>> {
        Ok(Box::new(FullTextScanLocalState::default()))
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
            .downcast_ref::<FullTextScanGlobalState>()
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

#[cfg(test)]
mod tests {
    use super::{FullTextExecMode, FullTextScanBindData, PhysicalFullTextScan};
    use crate::operator::PhysicalOperator;
    use paro_common::chunk::Chunk;
    use paro_common::types::LogicalType;
    use paro_common::vector::Vector;
    use paro_storage::table::table_factory::TableFactory;
    use paro_storage::table::table_handle::TableHandle;
    use std::collections::BTreeSet;
    use std::sync::Arc;

    fn collect_ids(chunks: &[Chunk]) -> Vec<i32> {
        let mut ids = Vec::new();
        for chunk in chunks {
            let col = chunk.column(0).expect("id column");
            for row in 0..chunk.size() {
                ids.push(col.get_i32(row).expect("id as i32"));
            }
        }
        ids
    }

    fn collect_scores(chunks: &[Chunk]) -> Vec<f32> {
        let mut scores = Vec::new();
        for chunk in chunks {
            let col = chunk.column(1).expect("score column");
            for row in 0..chunk.size() {
                scores.push(col.get_f32(row).expect("score as f32"));
            }
        }
        scores
    }

    fn setup_fulltext_table() -> Arc<TableHandle> {
        let table = Arc::new(
            TableFactory::default()
                .create_table(&[LogicalType::Integer, LogicalType::Varchar])
                .unwrap(),
        );
        table
            .append(&Chunk::from_vectors(vec![
                Vector::from_i32(&[1, 2, 3, 4, 5]),
                Vector::from_strings(&[
                    "vector database vector",
                    "vector database",
                    "database vector",
                    "vector",
                    "noise",
                ]),
            ]))
            .expect("append");
        table
            .build_runtime_fulltext_index(1)
            .expect("build fulltext");
        table
    }

    #[test]
    fn filter_mode_matches_score_topk_hit_set_and_ignores_k() {
        let table = setup_fulltext_table();

        let filter_scan = PhysicalFullTextScan::new(
            FullTextScanBindData::new(table.clone(), "vector database".to_string(), 1, 1, vec![0])
                .with_exec_mode(FullTextExecMode::Filter),
        );
        let filter_chunks = filter_scan.execute_search().expect("filter execute");
        let filter_ids: BTreeSet<i32> = collect_ids(&filter_chunks).into_iter().collect();
        assert_eq!(filter_ids, BTreeSet::from([1, 2, 3]));
        assert_eq!(filter_scan.estimated_cardinality(), table.total_rows());

        let score_scan_all = PhysicalFullTextScan::new(
            FullTextScanBindData::new(table.clone(), "vector database".to_string(), 10, 1, vec![0])
                .with_exec_mode(FullTextExecMode::ScoreTopK),
        );
        let score_chunks_all = score_scan_all.execute_search().expect("score execute");
        let score_ids_all: BTreeSet<i32> = collect_ids(&score_chunks_all).into_iter().collect();
        assert_eq!(score_ids_all, filter_ids);

        let score_scan_top1 = PhysicalFullTextScan::new(
            FullTextScanBindData::new(table, "vector database".to_string(), 1, 1, vec![0])
                .with_exec_mode(FullTextExecMode::ScoreTopK),
        );
        let score_chunks_top1 = score_scan_top1.execute_search().expect("score top1");
        let score_ids_top1: BTreeSet<i32> = collect_ids(&score_chunks_top1).into_iter().collect();
        assert_eq!(score_ids_top1.len(), 1);
        assert!(score_ids_top1.is_subset(&score_ids_all));
    }

    #[test]
    fn score_topk_emit_score_outputs_rank_column() {
        let table = setup_fulltext_table();

        let scan = PhysicalFullTextScan::new(
            FullTextScanBindData::new(table, "vector database".to_string(), 10, 1, vec![0])
                .with_exec_mode(FullTextExecMode::ScoreTopK)
                .with_emit_score(true),
        );

        assert_eq!(scan.types(), &[LogicalType::Integer, LogicalType::Float]);

        let chunks = scan.execute_search().expect("score execute");
        let ids = collect_ids(&chunks);
        let scores = collect_scores(&chunks);
        assert_eq!(ids.len(), scores.len());
        assert!(!scores.is_empty());
        assert!(scores.iter().all(|s| *s >= 0.0));
        for pair in scores.windows(2) {
            assert!(pair[0] >= pair[1]);
        }
    }
}
