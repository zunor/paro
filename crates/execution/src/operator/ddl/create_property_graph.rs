// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Physical Create Property Graph Operator
//!
//! Executes CREATE PROPERTY GRAPH DDL:
//! 1. Scans vertex/edge tables to build GraphProjectionIndex
//! 2. Persists the build into a transaction-private staging directory
//! 3. Records a typed catalog op + staged-artifact descriptor
//! 4. Defers publish/register until post-commit

use super::property_graph_support::{build_graph_index, graph_staging_dir};
use crate::execution_context::ExecutionContext;
use crate::operator::state::OperatorSourceInput;
use crate::operator::PhysicalOperator;
use crate::operator_type::PhysicalOperatorType;
use crate::result_type::SourceResultType;
use paro_catalog::catalog::Catalog;
use paro_catalog::entry::graph_schema_fingerprint;
use paro_common::chunk::Chunk;
use paro_common::effect::StagingArtifactId;
use paro_common::error::{self as paro_error, Result};
use paro_common::types::LogicalType;
use paro_planner::binder::ir::statement::BoundCreatePropertyGraphInfo;
use paro_storage::index::graph::{
    GraphManifest, GraphProjectionIndex, GraphState, GraphStatistics,
};
use std::any::Any;
use std::path::{Path, PathBuf};

#[derive(Debug)]
pub struct CreatePropertyGraph {
    pub info: BoundCreatePropertyGraphInfo,
}

impl CreatePropertyGraph {
    pub fn new(info: BoundCreatePropertyGraphInfo) -> Self {
        Self { info }
    }

    /// Compute the transaction-private staging directory path.
    fn graph_staging_dir(&self, db_path: &str, txn_id: u64) -> PathBuf {
        graph_staging_dir(db_path, txn_id, &self.info.info.graph_name)
    }

    fn write_manifest(
        &self,
        graph_dir: &Path,
        state: GraphState,
        schema_fingerprint: &str,
    ) -> Result<()> {
        let manifest = GraphManifest::new(
            self.info.info.graph_name.clone(),
            state,
            schema_fingerprint.to_string(),
        );
        GraphProjectionIndex::write_manifest(graph_dir, &manifest)
    }
}

impl PhysicalOperator for CreatePropertyGraph {
    fn operator_type(&self) -> PhysicalOperatorType {
        PhysicalOperatorType::CreatePropertyGraph
    }

    fn types(&self) -> &[LogicalType] {
        &[]
    }

    fn is_source(&self) -> bool {
        true
    }

    fn get_data(
        &self,
        ctx: &ExecutionContext,
        _chunk: &mut Chunk,
        _input: &mut OperatorSourceInput,
    ) -> Result<SourceResultType> {
        let pg_info = &self.info.info;

        // IF NOT EXISTS short-circuit
        if pg_info.if_not_exists {
            let catalog = ctx.session.catalog();
            let txn = ctx.session.catalog_txn_view();
            let schema = catalog.get_schema(&txn, &pg_info.schema)?;
            if schema.get_property_graph(&txn, &pg_info.graph_name).is_ok() {
                return Ok(SourceResultType::Finished);
            }
        }

        let db_path = ctx.session.catalog().get_db_path();
        let txn_id = ctx
            .session
            .active_transaction()
            .ok_or_else(|| {
                paro_error::internal("CREATE PROPERTY GRAPH requires an active transaction")
            })?
            .id;
        let graph_dir = self.graph_staging_dir(&db_path, txn_id);
        if graph_dir.exists() {
            let _ = std::fs::remove_dir_all(&graph_dir);
        }

        let schema_fingerprint = graph_schema_fingerprint(pg_info);
        self.write_manifest(&graph_dir, GraphState::Building, &schema_fingerprint)?;

        let result: Result<SourceResultType> = (|| {
            // 1. Build the graph projection index
            let index = build_graph_index(ctx, pg_info)?;
            let graph_stats = GraphStatistics::from_index(&index);

            // 2. Persist to disk with READY manifest
            let manifest = GraphManifest::new(
                pg_info.graph_name.clone(),
                GraphState::Ready,
                schema_fingerprint.clone(),
            )
            .with_indexed_through_ts(ctx.session.txn.transaction.visible_version())
            .with_statistics(graph_stats);
            index.save_with_manifest(&graph_dir, manifest.clone())?;
            let _ = (index, manifest);

            let ddl = ctx.session.ddl().ok_or_else(|| {
                paro_error::internal("property graph DDL requires transaction DDL context")
            })?;
            ddl.apply_create_property_graph(
                pg_info.clone(),
                StagingArtifactId::new(
                    txn_id,
                    graph_dir
                        .components()
                        .filter_map(|component| match component {
                            std::path::Component::Normal(value) => {
                                Some(value.to_string_lossy().to_string())
                            }
                            std::path::Component::RootDir => Some("/".to_string()),
                            _ => None,
                        })
                        .collect(),
                ),
                schema_fingerprint.clone(),
            )?;

            Ok(SourceResultType::Finished)
        })();

        if let Err(error) = &result {
            let _ = self.write_manifest(&graph_dir, GraphState::Failed, &schema_fingerprint);
            return Err(error.clone());
        }

        result
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}
