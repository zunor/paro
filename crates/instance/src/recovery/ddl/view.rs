use crate::recovery::replay_handler::CatalogReplayHandler;
use paro_catalog::collection::InstallMode;
use paro_catalog::entry::CatalogObjectId;
use paro_catalog::entry::{CatalogEntryEnum, CatalogType, CreateViewInfo, ViewCatalogEntry};
use paro_common::ddl::CreateViewPayload;
use paro_common::error as paro_error;
use paro_common::logging::targets;
use paro_parser::ast::Statement;
use paro_parser::parse_one;
use std::sync::Arc;

impl<'a> CatalogReplayHandler<'a> {
    pub(in crate::recovery) fn replay_create_view(
        &mut self,
        schema_name: &str,
        view_name: &str,
        payload: &CreateViewPayload,
        commit_id: u64,
    ) -> paro_common::error::Result<()> {
        let schema = self.ensure_schema(schema_name, commit_id)?;
        if schema
            .get_view(
                self.transaction.transaction_id,
                self.transaction.start_time,
                view_name,
            )
            .is_some()
        {
            tracing::debug!(
                target: targets::INSTANCE,
                schema = schema_name,
                view = view_name,
                "View already exists, skipping"
            );
            return Ok(());
        }

        let statement = parse_one(&payload.sql).map_err(|err| {
            paro_error::serialization_error(format!("failed to parse CREATE VIEW SQL: {err}"))
        })?;
        let (query, aliases_from_sql) = match statement.stmt {
            Statement::Query(query) => (query, Vec::new()),
            Statement::CreateView(stmt) => (
                stmt.query,
                stmt.columns
                    .into_iter()
                    .map(|identifier| identifier.name)
                    .collect(),
            ),
            _ => {
                return Err(paro_error::serialization_error(
                    "CREATE VIEW replay expected CREATE VIEW or query SQL payload",
                ))
            }
        };
        let column_aliases = if payload.column_aliases.is_empty() {
            aliases_from_sql
        } else {
            payload.column_aliases.clone()
        };
        let info = CreateViewInfo::new(schema_name.to_string(), view_name.to_string(), query)
            .with_catalog(self.catalog.name().to_string())
            .with_aliases(column_aliases)
            .with_dependencies(Self::dependency_list_from_payload(&payload.dependencies)?)
            .with_sql(payload.sql.clone());
        self.observe_object_id(payload.object_id);
        let entry = Arc::new(CatalogEntryEnum::View(Arc::new(
            ViewCatalogEntry::with_object_id(
                info,
                0,
                self.catalog.name().to_string(),
                CatalogObjectId::from_raw(payload.object_id),
            ),
        )));
        let view_collection = schema
            .collection(CatalogType::View)
            .expect("view collection");
        self.install_replayed_entry(
            view_collection,
            commit_id,
            entry,
            InstallMode::RejectExisting,
        )?;
        tracing::info!(
            target: targets::INSTANCE,
            schema = schema_name,
            view = view_name,
            "Replayed CREATE VIEW"
        );
        Ok(())
    }

    pub(in crate::recovery) fn replay_drop_view(
        &mut self,
        schema_name: &str,
        view_name: &str,
        _if_exists: bool,
        commit_id: u64,
    ) -> paro_common::error::Result<()> {
        let schema = match self.catalog.get_schema(&self.transaction, schema_name) {
            Ok(schema) => schema,
            Err(_) => {
                tracing::debug!(
                    target: targets::INSTANCE,
                    schema = schema_name,
                    view = view_name,
                    "DROP VIEW replay skipped: schema not found"
                );
                return Ok(());
            }
        };

        if let Some(handle) = schema
            .collection(CatalogType::View)
            .expect("view collection")
            .stage_drop(&self.transaction, view_name)?
        {
            self.publish_catalog_handle(handle, commit_id)?;
            tracing::info!(
                target: targets::INSTANCE,
                schema = schema_name,
                view = view_name,
                "Replayed DROP VIEW"
            );
        } else {
            tracing::debug!(
                target: targets::INSTANCE,
                schema = schema_name,
                view = view_name,
                "DROP VIEW replay skipped: already absent"
            );
        }
        Ok(())
    }
}
