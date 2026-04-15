use crate::recovery::replay_handler::CatalogReplayHandler;
use paro_catalog::collection::InstallMode;
use paro_catalog::entry::CatalogObjectId;
use paro_catalog::entry::{
    CatalogEntryEnum, CatalogType, CreatePropertyGraphInfo, EdgeTableInfo,
    PropertyGraphCatalogEntry, VertexTableInfo,
};
use paro_common::ddl::CreatePropertyGraphPayload;
use paro_common::logging::targets;
use std::sync::Arc;

impl<'a> CatalogReplayHandler<'a> {
    pub(in crate::recovery) fn replay_create_property_graph(
        &mut self,
        schema_name: &str,
        payload: &CreatePropertyGraphPayload,
        commit_id: u64,
    ) -> paro_common::error::Result<()> {
        let schema = self.ensure_schema(schema_name, commit_id)?;
        let info = CreatePropertyGraphInfo {
            catalog: self.catalog.name().to_string(),
            schema: payload.schema.clone(),
            graph_name: payload.graph_name.clone(),
            if_not_exists: payload.if_not_exists,
            vertex_tables: payload
                .vertex_tables
                .iter()
                .map(|vertex| VertexTableInfo {
                    table_name: vertex.table_name.clone(),
                    table_oid: vertex.table_oid,
                    key_column_ids: vertex.key_column_ids.clone(),
                    label: vertex.label.clone(),
                    property_column_ids: vertex.property_column_ids.clone(),
                })
                .collect(),
            edge_tables: payload
                .edge_tables
                .iter()
                .map(|edge| EdgeTableInfo {
                    table_name: edge.table_name.clone(),
                    table_oid: edge.table_oid,
                    key_column_ids: edge.key_column_ids.clone(),
                    source_key_column_ids: edge.source_key_column_ids.clone(),
                    source_vertex_table: edge.source_vertex_table.clone(),
                    source_ref_column_ids: edge.source_ref_column_ids.clone(),
                    destination_key_column_ids: edge.destination_key_column_ids.clone(),
                    destination_vertex_table: edge.destination_vertex_table.clone(),
                    destination_ref_column_ids: edge.destination_ref_column_ids.clone(),
                    label: edge.label.clone(),
                    property_column_ids: edge.property_column_ids.clone(),
                })
                .collect(),
        };
        self.observe_object_id(payload.object_id);
        let entry = Arc::new(CatalogEntryEnum::PropertyGraph(Arc::new(
            PropertyGraphCatalogEntry::with_object_id(
                info,
                0,
                self.catalog.name().to_string(),
                CatalogObjectId::from_raw(payload.object_id),
            ),
        )));
        let graph_collection = schema
            .collection(CatalogType::PropertyGraph)
            .expect("property graph collection");
        self.install_replayed_entry(
            graph_collection,
            commit_id,
            entry,
            InstallMode::RejectExisting,
        )?;
        Ok(())
    }

    pub(in crate::recovery) fn replay_drop_property_graph(
        &mut self,
        schema_name: &str,
        graph_name: &str,
        _if_exists: bool,
        commit_id: u64,
    ) -> paro_common::error::Result<()> {
        let schema = match self.catalog.get_schema(&self.transaction, schema_name) {
            Ok(schema) => schema,
            Err(_) => {
                tracing::debug!(
                    target: targets::INSTANCE,
                    schema = schema_name,
                    graph = graph_name,
                    "DROP PROPERTY GRAPH replay skipped: schema not found"
                );
                return Ok(());
            }
        };

        if let Some(handle) = schema
            .collection(CatalogType::PropertyGraph)
            .expect("property graph collection")
            .stage_drop(&self.transaction, graph_name)?
        {
            self.publish_catalog_handle(handle, commit_id)?;
        }
        Ok(())
    }
}
