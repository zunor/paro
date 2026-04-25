// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Physical full-text scan operator backed by a storage-owned search cursor.

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
use paro_planner::operator::{FullTextQueryStats, FullTextScoreMode};
use paro_storage::index::fulltext::query_parser::{
    parse_phraseto_tsquery, parse_plainto_tsquery, parse_query, parse_to_tsquery,
    parse_websearch_to_tsquery, ParsedQuery,
};
use paro_storage::index::fulltext::text_index::GlobalFullTextStats;
use paro_storage::index::fulltext::tokenizer::{tokenizer_from_config, Tokenizer};
use paro_storage::index::PredicateTree;
#[cfg(test)]
use paro_storage::search::{ResourceBudget, SearchBatchConfig, SearchBatchState};
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
    pub score_mode: FullTextScoreMode,
    pub query_stats: FullTextQueryStats,
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
            score_mode: FullTextScoreMode::default(),
            query_stats: FullTextQueryStats::new(1),
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

    pub fn with_score_mode(mut self, score_mode: FullTextScoreMode) -> Self {
        self.score_mode = score_mode;
        self
    }

    pub fn with_query_stats(mut self, query_stats: FullTextQueryStats) -> Self {
        self.query_stats = query_stats;
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

    fn collect_global_stats(&self) -> Option<GlobalFullTextStats> {
        self.bind_data
            .table_data
            .fulltext_capability(self.bind_data.text_column_id as u32, &self.bind_data.config)?
            .generation_stats
            .fulltext_global_stats()
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

    #[cfg(test)]
    fn collect_test_chunks(&self) -> Result<Vec<Chunk>> {
        let visible_version = self.bind_data.table_data.max_version();
        let parsed_query = self.parse_query()?;
        let opened = match self.bind_data.mode {
            FullTextExecMode::Filter => self.bind_data.table_data.open_fulltext_filter_cursor(
                self.bind_data.text_column_id,
                &parsed_query,
                &self.bind_data.config,
                self.bind_data.predicates.clone(),
                visible_version,
            )?,
            FullTextExecMode::ScoreTopK => self.bind_data.table_data.open_fulltext_search_cursor(
                self.bind_data.text_column_id,
                &parsed_query,
                self.bind_data.k,
                &self.bind_data.config,
                self.bind_data.predicates.clone(),
                self.collect_global_stats(),
                self.bind_data.score_mode,
                visible_version,
            )?,
        };

        let row_limit_hint = match self.bind_data.mode {
            FullTextExecMode::Filter => 1024,
            FullTextExecMode::ScoreTopK => self.bind_data.k,
        };
        let batch_config = SearchBatchConfig {
            row_limit: row_limit_hint.max(1).min(1024),
            preferred_bytes: 1 << 20,
        };
        let mut budget = ResourceBudget {
            memory_limit_bytes: 64 * 1024 * 1024,
            heap_budget_items: row_limit_hint.max(1024),
            parallelism_slots: 4,
            cpu_step_budget: None,
            context: None,
        };
        let mut cursor = opened.cursor;
        let snapshot = opened.snapshot;
        let mut chunks = Vec::new();
        loop {
            match cursor.next_batch(&batch_config, &mut budget)? {
                SearchBatchState::Ready(batch) if batch.is_empty() => continue,
                SearchBatchState::Ready(batch) => {
                    chunks.push(self.bind_data.table_data.materialize_search_batch(
                        &snapshot,
                        batch,
                        &self.bind_data.projected_columns,
                        self.bind_data.emit_score
                            && matches!(self.bind_data.mode, FullTextExecMode::ScoreTopK),
                    )?)
                }
                SearchBatchState::Exhausted => return Ok(chunks),
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
        ctx: &ExecutionContext,
        _sink_state: Option<&dyn crate::operator::state::GlobalSinkState>,
    ) -> Result<Box<dyn GlobalSourceState>> {
        ctx.check_cancelled()?;
        let visible_version = i64::try_from(ctx.transaction_visible_version()).unwrap_or(i64::MAX);
        let parsed_query = self.parse_query()?;
        let opened = match self.bind_data.mode {
            FullTextExecMode::Filter => self.bind_data.table_data.open_fulltext_filter_cursor(
                self.bind_data.text_column_id,
                &parsed_query,
                &self.bind_data.config,
                self.bind_data.predicates.clone(),
                visible_version,
            )?,
            FullTextExecMode::ScoreTopK => self.bind_data.table_data.open_fulltext_search_cursor(
                self.bind_data.text_column_id,
                &parsed_query,
                self.bind_data.k,
                &self.bind_data.config,
                self.bind_data.predicates.clone(),
                self.collect_global_stats(),
                self.bind_data.score_mode,
                visible_version,
            )?,
        };
        let row_limit_hint = match self.bind_data.mode {
            FullTextExecMode::Filter => 1024,
            FullTextExecMode::ScoreTopK => self.bind_data.k,
        };
        let heap_budget_items = match self.bind_data.mode {
            FullTextExecMode::Filter => 1024,
            FullTextExecMode::ScoreTopK => self.bind_data.k,
        };
        let driver = SearchOperatorDriver::new(
            self.bind_data.table_data.clone(),
            opened,
            build_search_batch_config(row_limit_hint),
            build_search_resource_budget(ctx, heap_budget_items),
            self.bind_data.projected_columns.clone(),
            self.bind_data.emit_score && matches!(self.bind_data.mode, FullTextExecMode::ScoreTopK),
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
    use super::{FullTextExecMode, FullTextScanBindData, PhysicalFullTextScan};
    use crate::operator::PhysicalOperator;
    use paro_common::chunk::Chunk;
    use paro_common::types::LogicalType;

    use paro_planner::operator::{FullTextQueryStats, FullTextScoreMode};
    use paro_storage::search::{SearchIndexDefinition, SearchIndexKind};
    use paro_storage::table::table_factory::TableFactory;
    use paro_storage::table::table_handle::TableHandle;
    use serde_json::json;
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
        let provider_config = json!({ "config": "simple" });
        let expression = "to_tsvector('simple', col_1)".to_string();
        let definition = SearchIndexDefinition {
            definition_id: 1,
            table_id: table.tablet().table_id(),
            name: "__test_fulltext_search_simple".to_string(),
            kind: SearchIndexKind::FullText,
            column_ids: vec![1],
            expression: Some(expression.clone()),
            config_fingerprint: SearchIndexDefinition::compute_config_fingerprint(
                SearchIndexKind::FullText,
                &[1],
                Some(&expression),
                &provider_config,
            ),
            provider_config,
        };
        table
            .register_search_definition(definition)
            .expect("register fulltext definition");
        table
            .append(&Chunk::from_vectors(
                vec![
                    paro_common::test_utils::test_i32_vector_with_allocator(
                        &[1, 2, 3, 4, 5],
                        paro_common::test_utils::test_allocator(),
                    ),
                    paro_common::test_utils::test_string_vector_with_allocator(
                        &[
                            "vector database vector",
                            "vector database",
                            "database vector",
                            "vector",
                            "noise",
                        ],
                        paro_common::test_utils::test_allocator(),
                    ),
                ],
                paro_common::test_utils::test_allocator(),
            ))
            .expect("append");
        table
    }

    fn setup_empty_fulltext_table() -> Arc<TableHandle> {
        let table = Arc::new(
            TableFactory::default()
                .create_table(&[LogicalType::Integer, LogicalType::Varchar])
                .unwrap(),
        );
        let provider_config = json!({ "config": "simple" });
        let expression = "to_tsvector('simple', col_1)".to_string();
        let definition = SearchIndexDefinition {
            definition_id: 9,
            table_id: table.tablet().table_id(),
            name: "__test_fulltext_search_empty".to_string(),
            kind: SearchIndexKind::FullText,
            column_ids: vec![1],
            expression: Some(expression.clone()),
            config_fingerprint: SearchIndexDefinition::compute_config_fingerprint(
                SearchIndexKind::FullText,
                &[1],
                Some(&expression),
                &provider_config,
            ),
            provider_config,
        };
        table
            .register_search_definition(definition)
            .expect("register empty fulltext definition");
        table
    }

    #[test]
    fn filter_mode_matches_score_topk_hit_set_and_ignores_k() {
        let table = setup_fulltext_table();

        let filter_scan = PhysicalFullTextScan::new(
            FullTextScanBindData::new(table.clone(), "vector database".to_string(), 1, 1, vec![0])
                .with_exec_mode(FullTextExecMode::Filter),
        );
        let filter_chunks = filter_scan.collect_test_chunks().expect("filter execute");
        let filter_ids: BTreeSet<i32> = collect_ids(&filter_chunks).into_iter().collect();
        assert_eq!(filter_ids, BTreeSet::from([1, 2, 3]));
        assert_eq!(filter_scan.estimated_cardinality(), table.total_rows());

        let score_scan_all = PhysicalFullTextScan::new(
            FullTextScanBindData::new(table.clone(), "vector database".to_string(), 10, 1, vec![0])
                .with_exec_mode(FullTextExecMode::ScoreTopK),
        );
        let score_chunks_all = score_scan_all.collect_test_chunks().expect("score execute");
        let score_ids_all: BTreeSet<i32> = collect_ids(&score_chunks_all).into_iter().collect();
        assert_eq!(score_ids_all, filter_ids);

        let score_scan_top1 = PhysicalFullTextScan::new(
            FullTextScanBindData::new(table, "vector database".to_string(), 1, 1, vec![0])
                .with_exec_mode(FullTextExecMode::ScoreTopK),
        );
        let score_chunks_top1 = score_scan_top1.collect_test_chunks().expect("score top1");
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

        let chunks = scan.collect_test_chunks().expect("score execute");
        let ids = collect_ids(&chunks);
        let scores = collect_scores(&chunks);
        assert_eq!(ids.len(), scores.len());
        assert!(!scores.is_empty());
        assert!(scores.iter().all(|s| *s >= 0.0));
        for pair in scores.windows(2) {
            assert!(pair[0] >= pair[1]);
        }
    }

    #[test]
    fn bind_data_preserves_score_mode_and_query_stats() {
        let table = setup_fulltext_table();
        let scan = PhysicalFullTextScan::new(
            FullTextScanBindData::new(table, "vector database".to_string(), 10, 1, vec![0])
                .with_exec_mode(FullTextExecMode::ScoreTopK)
                .with_score_mode(FullTextScoreMode::CoverDensity)
                .with_query_stats(FullTextQueryStats::new(2)),
        );

        assert!(matches!(
            scan.bind_data.score_mode,
            FullTextScoreMode::CoverDensity
        ));
        assert_eq!(scan.bind_data.query_stats.effective_query_terms(), 2);
    }

    #[test]
    fn empty_generation_collects_global_stats_from_capability_contract() {
        let table = setup_empty_fulltext_table();
        let scan = PhysicalFullTextScan::new(
            FullTextScanBindData::new(table, "vector".to_string(), 1, 1, vec![0])
                .with_exec_mode(FullTextExecMode::ScoreTopK),
        );

        let stats = scan
            .collect_global_stats()
            .expect("empty generation should still expose zeroed global stats");
        assert_eq!(stats.total_docs, 0);
        assert_eq!(stats.total_terms, 0);
        assert_eq!(stats.avg_doc_length, 0.0);
    }
}
