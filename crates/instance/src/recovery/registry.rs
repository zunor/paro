// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use paro_catalog::catalog::DEFAULT_SCHEMA;
use paro_catalog::database_catalog::ParoCatalog;
use paro_catalog::entry::{CatalogEntryEnum, CatalogType};
use paro_catalog::mvcc::CatalogSnapshot;
use paro_common::ddl::{DdlObjectKey, DdlObjectKind};
use paro_common::error::Result;
use paro_common::types::LogicalType;
use paro_storage::table::table_factory::TableFactory;
use paro_storage::table::table_handle::TableHandle;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::Mutex;

#[derive(Debug, Clone)]
pub struct TabletRoute {
    pub schema_name: String,
    pub table_name: String,
    pub storage: Arc<TableHandle>,
}

#[derive(Debug, Clone, Default)]
struct RouteMaps {
    tablet_by_id: HashMap<u64, TabletRoute>,
    tablet_id_by_key: HashMap<DdlObjectKey, u64>,
    table_keys_by_tablet: HashMap<u64, HashSet<DdlObjectKey>>,
}

#[derive(Debug, Default)]
struct RouteRuntimeState {
    rowset_owner: Mutex<HashMap<u64, u64>>,
    rowsets_by_tablet: Mutex<HashMap<u64, HashSet<u64>>>,
    tablet_applied_lsn: Mutex<HashMap<u64, u64>>,
}

#[derive(Debug, Clone)]
pub struct RouteRegistry {
    routes: Arc<RouteMaps>,
    runtime: Arc<RouteRuntimeState>,
}

impl Default for RouteRegistry {
    fn default() -> Self {
        Self {
            routes: Arc::new(RouteMaps::default()),
            runtime: Arc::new(RouteRuntimeState::default()),
        }
    }
}

impl RouteRegistry {
    pub fn from_catalog(catalog: &Arc<ParoCatalog>) -> Result<Self> {
        let mut registry = Self::default();

        let txn = CatalogSnapshot::default();
        for schema_entry in catalog
            .get_schema_collection()
            .scan(txn.transaction_id, txn.start_time)
        {
            let CatalogEntryEnum::Schema(schema) = schema_entry.as_ref() else {
                continue;
            };

            for table_entry in schema
                .collection(CatalogType::Table)
                .expect("table collection")
                .scan(txn.transaction_id, txn.start_time)
            {
                let CatalogEntryEnum::Table(table) = table_entry.as_ref() else {
                    continue;
                };

                let key = DdlObjectKey::new(
                    table.database_name(),
                    Some(schema.base.name.clone()),
                    table.name(),
                    DdlObjectKind::Table,
                );
                registry.sync_table_from_catalog(catalog, &key)?;
            }
        }

        Ok(registry)
    }

    pub fn route_tablet(&self, tablet_id: u64) -> Option<&TabletRoute> {
        self.routes.tablet_by_id.get(&tablet_id)
    }

    pub fn route_table_key(&self, key: &DdlObjectKey) -> Option<&TabletRoute> {
        self.routes
            .tablet_id_by_key
            .get(key)
            .and_then(|tablet_id| self.routes.tablet_by_id.get(tablet_id))
    }

    pub fn tablet_routes(&self) -> Vec<TabletRoute> {
        self.routes.tablet_by_id.values().cloned().collect()
    }

    pub fn table_keys_in_schema(&self, database: &str, schema: &str) -> Vec<DdlObjectKey> {
        self.routes
            .tablet_id_by_key
            .keys()
            .filter(|key| {
                key.kind == DdlObjectKind::Table
                    && key.database == database
                    && key.schema.as_deref() == Some(schema)
            })
            .cloned()
            .collect()
    }

    pub fn route_rowset(&self, rowset_id: u64) -> Option<&TabletRoute> {
        self.runtime
            .rowset_owner
            .lock()
            .unwrap()
            .get(&rowset_id)
            .and_then(|tablet_id| self.routes.tablet_by_id.get(tablet_id))
    }

    pub fn sync_table_from_catalog(
        &mut self,
        catalog: &Arc<ParoCatalog>,
        key: &DdlObjectKey,
    ) -> Result<()> {
        if key.kind != DdlObjectKind::Table {
            return Ok(());
        }

        let Some((route, rowset_ids)) = Self::open_table_route(catalog, key)? else {
            self.remove_table_key(key);
            return Ok(());
        };

        let tablet_id = route.storage.tablet_id();
        self.upsert_table_route(key.clone(), route);
        self.replace_tablet_rowsets(tablet_id, rowset_ids);
        self.note_tablet_applied_lsn(
            tablet_id,
            self.routes.tablet_by_id[&tablet_id]
                .storage
                .tablet()
                .applied_lsn(),
        );
        Ok(())
    }

    pub fn note_rowset_owner(&self, rowset_id: u64, tablet_id: u64) {
        let mut rowset_owner = self.runtime.rowset_owner.lock().unwrap();
        let mut rowsets_by_tablet = self.runtime.rowsets_by_tablet.lock().unwrap();
        if let Some(previous_tablet_id) = rowset_owner.insert(rowset_id, tablet_id) {
            if previous_tablet_id != tablet_id {
                Self::remove_rowset_from_tablet(
                    &mut rowsets_by_tablet,
                    previous_tablet_id,
                    rowset_id,
                );
            }
        }
        rowsets_by_tablet
            .entry(tablet_id)
            .or_default()
            .insert(rowset_id);
    }

    pub fn forget_rowset_owner(&self, rowset_id: u64) {
        let mut rowset_owner = self.runtime.rowset_owner.lock().unwrap();
        let mut rowsets_by_tablet = self.runtime.rowsets_by_tablet.lock().unwrap();
        let Some(tablet_id) = rowset_owner.remove(&rowset_id) else {
            return;
        };
        Self::remove_rowset_from_tablet(&mut rowsets_by_tablet, tablet_id, rowset_id);
    }

    pub fn note_tablet_applied_lsn(&self, tablet_id: u64, lsn: u64) {
        self.runtime
            .tablet_applied_lsn
            .lock()
            .unwrap()
            .entry(tablet_id)
            .and_modify(|current| *current = (*current).max(lsn))
            .or_insert(lsn);
    }

    pub fn tablet_applied_lsn(&self, tablet_id: u64) -> Option<u64> {
        self.runtime
            .tablet_applied_lsn
            .lock()
            .unwrap()
            .get(&tablet_id)
            .copied()
    }

    fn open_table_route(
        catalog: &Arc<ParoCatalog>,
        key: &DdlObjectKey,
    ) -> Result<Option<(TabletRoute, Vec<u64>)>> {
        let schema_name = key.schema.as_deref().unwrap_or(DEFAULT_SCHEMA);
        let txn = CatalogSnapshot::default();
        let Ok(schema) = catalog.get_schema(&txn, schema_name) else {
            return Ok(None);
        };
        let Some(table_entry) = schema.get_table(txn.transaction_id, txn.start_time, &key.name)
        else {
            return Ok(None);
        };
        let CatalogEntryEnum::Table(table) = table_entry.as_ref() else {
            return Ok(None);
        };

        let descriptor = table.get_storage_descriptor().cloned().or_else(|| {
            table
                .get_storage()
                .and_then(|storage| storage.to_descriptor().ok())
        });
        let Some(descriptor) = descriptor else {
            return Ok(None);
        };

        let storage = if let Some(storage) = table.get_storage() {
            Arc::clone(storage)
        } else {
            let column_types: Vec<LogicalType> = table
                .columns
                .iter()
                .map(|column| column.logical_type.clone())
                .collect();
            Arc::new(TableFactory::default().open_from_descriptor(&column_types, &descriptor)?)
        };
        let rowset_ids = storage
            .tablet()
            .capture_consistent_rowsets(i64::MAX)?
            .into_iter()
            .map(|rowset| rowset.rowset_id())
            .collect();
        Ok(Some((
            TabletRoute {
                schema_name: table.base.schema_name.clone(),
                table_name: table.base.base.name.clone(),
                storage,
            },
            rowset_ids,
        )))
    }

    fn upsert_table_route(&mut self, key: DdlObjectKey, route: TabletRoute) {
        let tablet_id = route.storage.tablet_id();
        let routes = Arc::make_mut(&mut self.routes);
        if let Some(previous_tablet_id) = routes.tablet_id_by_key.insert(key.clone(), tablet_id) {
            if previous_tablet_id != tablet_id {
                let remove_previous =
                    if let Some(keys) = routes.table_keys_by_tablet.get_mut(&previous_tablet_id) {
                        keys.remove(&key);
                        keys.is_empty()
                    } else {
                        false
                    };
                if remove_previous {
                    routes.table_keys_by_tablet.remove(&previous_tablet_id);
                    routes.tablet_by_id.remove(&previous_tablet_id);
                }
            }
        }
        routes
            .table_keys_by_tablet
            .entry(tablet_id)
            .or_default()
            .insert(key);
        routes.tablet_by_id.insert(tablet_id, route);
    }

    fn remove_table_key(&mut self, key: &DdlObjectKey) {
        let mut removed_tablet_id = None;
        {
            let routes = Arc::make_mut(&mut self.routes);
            let Some(tablet_id) = routes.tablet_id_by_key.remove(key) else {
                return;
            };
            let remove_tablet = if let Some(keys) = routes.table_keys_by_tablet.get_mut(&tablet_id)
            {
                keys.remove(key);
                keys.is_empty()
            } else {
                false
            };
            if remove_tablet {
                routes.table_keys_by_tablet.remove(&tablet_id);
                routes.tablet_by_id.remove(&tablet_id);
                removed_tablet_id = Some(tablet_id);
            }
        }
        if let Some(tablet_id) = removed_tablet_id {
            self.clear_tablet_runtime_state(tablet_id);
        }
    }

    fn replace_tablet_rowsets(&self, tablet_id: u64, rowset_ids: Vec<u64>) {
        let mut rowset_owner = self.runtime.rowset_owner.lock().unwrap();
        let mut rowsets_by_tablet = self.runtime.rowsets_by_tablet.lock().unwrap();
        if let Some(existing) = rowsets_by_tablet.remove(&tablet_id) {
            for rowset_id in existing {
                rowset_owner.remove(&rowset_id);
            }
        }

        if rowset_ids.is_empty() {
            return;
        }

        let mut current = HashSet::with_capacity(rowset_ids.len());
        for rowset_id in rowset_ids {
            if let Some(previous_tablet_id) = rowset_owner.insert(rowset_id, tablet_id) {
                if previous_tablet_id != tablet_id {
                    Self::remove_rowset_from_tablet(
                        &mut rowsets_by_tablet,
                        previous_tablet_id,
                        rowset_id,
                    );
                }
            }
            current.insert(rowset_id);
        }
        rowsets_by_tablet.insert(tablet_id, current);
    }

    fn clear_tablet_runtime_state(&self, tablet_id: u64) {
        let mut rowset_owner = self.runtime.rowset_owner.lock().unwrap();
        let mut rowsets_by_tablet = self.runtime.rowsets_by_tablet.lock().unwrap();
        if let Some(rowsets) = rowsets_by_tablet.remove(&tablet_id) {
            for rowset_id in rowsets {
                rowset_owner.remove(&rowset_id);
            }
        }
        self.runtime
            .tablet_applied_lsn
            .lock()
            .unwrap()
            .remove(&tablet_id);
    }

    fn remove_rowset_from_tablet(
        rowsets_by_tablet: &mut HashMap<u64, HashSet<u64>>,
        tablet_id: u64,
        rowset_id: u64,
    ) {
        if let Some(rowsets) = rowsets_by_tablet.get_mut(&tablet_id) {
            rowsets.remove(&rowset_id);
            if rowsets.is_empty() {
                rowsets_by_tablet.remove(&tablet_id);
            }
        }
    }
}
