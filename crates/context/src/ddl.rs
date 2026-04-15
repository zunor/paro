// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use paro_catalog::entry::{
    AlterEntryInfo, CreateIndexInfo, CreatePropertyGraphInfo, CreateSchemaInfo, CreateSequenceInfo,
    CreateTableInfo, CreateViewInfo, DropEntryInfo, IndexCoverage, TableCatalogEntry,
};
use paro_common::effect::StagingArtifactId;
use paro_common::error::Result;
use paro_storage::index::BoundIndex;
use std::any::Any;
use std::sync::Arc;

pub trait IndexBuildHandle: Any + Send + Sync {
    fn into_any(self: Box<Self>) -> Box<dyn Any>;
    fn skip_build(&self) -> bool;
}

pub enum PreparedIndexArtifact {
    RuntimeIndex {
        index: Arc<dyn BoundIndex>,
        coverage: Option<IndexCoverage>,
    },
    MetadataOnly {
        coverage: Option<IndexCoverage>,
    },
}

pub trait DdlApplyContext: Send + Sync {
    fn apply_create_table(&self, info: CreateTableInfo) -> Result<()>;
    fn apply_create_schema(&self, info: CreateSchemaInfo) -> Result<()>;
    fn apply_create_sequence(&self, info: CreateSequenceInfo) -> Result<()>;
    fn apply_create_view(&self, info: CreateViewInfo) -> Result<()>;
    fn apply_create_property_graph(
        &self,
        info: CreatePropertyGraphInfo,
        staging: StagingArtifactId,
        schema_fingerprint: String,
    ) -> Result<()>;
    fn prepare_index_build(
        &self,
        info: CreateIndexInfo,
        table: Arc<TableCatalogEntry>,
    ) -> Result<Box<dyn IndexBuildHandle>>;
    fn commit_index_build(
        &self,
        handle: Box<dyn IndexBuildHandle>,
        artifact: PreparedIndexArtifact,
    ) -> Result<()>;
    fn abort_index_build(&self, handle: Box<dyn IndexBuildHandle>, reason: String);
    fn apply_drop_property_graph(
        &self,
        catalog_name: String,
        schema_name: String,
        graph_name: String,
        if_exists: bool,
    ) -> Result<()>;
    fn apply_alter_entry(
        &self,
        schema_name: String,
        info: AlterEntryInfo,
        sql: String,
    ) -> Result<()>;
    fn apply_drop(&self, schema_name: String, info: DropEntryInfo) -> Result<()>;
}
