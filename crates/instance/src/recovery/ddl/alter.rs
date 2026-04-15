use crate::recovery::replay_handler::CatalogReplayHandler;
use paro_catalog::entry::{CatalogEntryEnum, CatalogType};
use paro_common::error as paro_error;
use paro_common::logging::targets;
use paro_parser::ast::{
    AlterTableAction, AlterTableStmt, ModifyColumnAction, RenameTableStmt, Statement,
    TableReference,
};
use paro_parser::parse_one;
use std::sync::Arc;

impl<'a> CatalogReplayHandler<'a> {
    pub(in crate::recovery) fn replay_alter_entry(
        &mut self,
        sql: &str,
        commit_id: u64,
    ) -> paro_common::error::Result<()> {
        let statement = parse_one(sql).map_err(|err| {
            paro_error::serialization_error(format!("failed to parse ALTER ENTRY SQL: {err}"))
        })?;
        match statement.stmt {
            Statement::AlterTable(stmt) => self.replay_alter_table(stmt, commit_id),
            Statement::RenameTable(stmt) => self.replay_rename_table(stmt, commit_id),
            other => Err(paro_error::not_supported(format!(
                "ALTER ENTRY replay does not support statement {:?}",
                other
            ))),
        }
    }

    fn replay_rename_table(
        &mut self,
        stmt: RenameTableStmt,
        commit_id: u64,
    ) -> paro_common::error::Result<()> {
        let RenameTableStmt {
            if_exists,
            database,
            schema,
            table,
            new_database,
            new_schema,
            new_table,
        } = stmt;

        let database_name = database
            .map(|ident| ident.name)
            .unwrap_or_else(|| self.catalog.name().to_string());
        if database_name != self.catalog.name() {
            return Err(paro_error::not_supported(format!(
                "cross-database RENAME TABLE replay is not supported ({database_name})",
            )));
        }
        if new_database.is_some() {
            return Err(paro_error::not_supported(
                "cross-database RENAME TABLE replay is not supported",
            ));
        }

        let schema_name = schema
            .map(|ident| ident.name)
            .unwrap_or_else(|| "public".to_string());
        let target_schema_name = new_schema
            .map(|ident| ident.name)
            .unwrap_or_else(|| schema_name.clone());
        let schema = self.catalog.get_schema(&self.transaction, &schema_name)?;
        let existing_table = schema.get_table(
            self.transaction.transaction_id,
            self.transaction.start_time,
            &table.name,
        );
        let Some(CatalogEntryEnum::Table(table_entry)) = existing_table.as_deref() else {
            if if_exists {
                return Ok(());
            }
            return Err(paro_error::object_not_found("table", &table.name));
        };

        let target_schema = if target_schema_name.eq_ignore_ascii_case(&schema_name) {
            Arc::clone(&schema)
        } else {
            self.catalog
                .get_schema(&self.transaction, &target_schema_name)?
        };
        let new_entry = Arc::new(CatalogEntryEnum::Table(Arc::new(
            table_entry.clone_with_new_schema_and_name(
                target_schema_name.clone(),
                new_table.name.clone(),
                commit_id,
            ),
        )));
        let handle = if target_schema_name.eq_ignore_ascii_case(&schema_name) {
            schema
                .collection(CatalogType::Table)
                .expect("table collection")
                .stage_rename(&self.transaction, &table.name, &new_table.name, new_entry)?
        } else {
            schema
                .collection(CatalogType::Table)
                .expect("table collection")
                .stage_move(
                    &self.transaction,
                    &table.name,
                    target_schema
                        .collection(CatalogType::Table)
                        .expect("table collection"),
                    &new_table.name,
                    new_entry,
                )?
        };
        let Some(handle) = handle else {
            if if_exists {
                return Ok(());
            }
            return Err(paro_error::object_not_found("table", &table.name));
        };
        self.publish_catalog_handle(handle, commit_id)?;
        tracing::info!(
            target: targets::INSTANCE,
            schema = schema_name,
            target_schema = target_schema_name,
            table = %table.name,
            renamed_table = %new_table.name,
            "Replayed RENAME TABLE"
        );
        Ok(())
    }

    fn replay_alter_table(
        &mut self,
        stmt: AlterTableStmt,
        commit_id: u64,
    ) -> paro_common::error::Result<()> {
        let TableReference::Table {
            database,
            schema,
            table,
            ..
        } = stmt.table_reference
        else {
            return Err(paro_error::not_supported(
                "ALTER TABLE replay only supports base tables",
            ));
        };

        let database_name = database
            .map(|ident| ident.name)
            .unwrap_or_else(|| self.catalog.name().to_string());
        if database_name != self.catalog.name() {
            return Err(paro_error::not_supported(format!(
                "cross-database ALTER TABLE replay is not supported ({database_name})",
            )));
        }

        let schema_name = schema
            .map(|ident| ident.name)
            .unwrap_or_else(|| "public".to_string());
        let schema = self.catalog.get_schema(&self.transaction, &schema_name)?;
        let existing_table = schema.get_table(
            self.transaction.transaction_id,
            self.transaction.start_time,
            &table.name,
        );
        let Some(CatalogEntryEnum::Table(table_entry)) = existing_table.as_deref() else {
            if stmt.if_exists {
                return Ok(());
            }
            return Err(paro_error::object_not_found("table", &table.name));
        };

        let new_entry = match stmt.action {
            AlterTableAction::RenameTable { new_table } => {
                let new_entry = Arc::new(CatalogEntryEnum::Table(Arc::new(
                    table_entry.clone_with_new_schema_and_name(
                        schema_name.clone(),
                        new_table.name.clone(),
                        commit_id,
                    ),
                )));
                let handle = schema
                    .collection(CatalogType::Table)
                    .expect("table collection")
                    .stage_rename(&self.transaction, &table.name, &new_table.name, new_entry)?;
                if let Some(handle) = handle {
                    self.publish_catalog_handle(handle, commit_id)?;
                    tracing::info!(
                        target: targets::INSTANCE,
                        schema = schema_name,
                        table = %table.name,
                        renamed_table = %new_table.name,
                        "Replayed ALTER TABLE RENAME"
                    );
                }
                return Ok(());
            }
            AlterTableAction::RenameColumn {
                old_column,
                new_column,
            } => Arc::new(CatalogEntryEnum::Table(Arc::new(
                table_entry.clone_with_renamed_column(
                    &old_column.name,
                    new_column.name,
                    commit_id,
                )?,
            ))),
            AlterTableAction::ModifyTableComment { new_comment } => {
                Arc::new(CatalogEntryEnum::Table(Arc::new(
                    table_entry.clone_with_comment(Some(new_comment), commit_id),
                )))
            }
            AlterTableAction::ModifyColumn {
                action: ModifyColumnAction::Comment(comments),
            } => {
                let updates = comments
                    .into_iter()
                    .map(|comment| (comment.name.name, comment.comment))
                    .collect::<Vec<_>>();
                Arc::new(CatalogEntryEnum::Table(Arc::new(
                    table_entry.clone_with_column_comments(&updates, commit_id)?,
                )))
            }
            other => {
                return Err(paro_error::not_supported(format!(
                    "ALTER TABLE replay does not support action {other}",
                )))
            }
        };

        if let Some(handle) = schema
            .collection(CatalogType::Table)
            .expect("table collection")
            .stage_replace(&self.transaction, &table.name, new_entry)?
        {
            self.publish_catalog_handle(handle, commit_id)?;
            tracing::info!(
                target: targets::INSTANCE,
                schema = schema_name,
                table = %table.name,
                "Replayed ALTER TABLE"
            );
        }
        Ok(())
    }
}
