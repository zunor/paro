// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! CREATE INDEX backfill lease and bounded catch-up state.

use paro_catalog::entry::{IndexCatalogEntry, TableCatalogEntry};
use paro_common::error::{self as paro_error, Result};
use paro_transaction::{BackfillLease, CommitTs, RetentionLeaseInfo};
use std::sync::{Arc, Mutex};

pub(crate) const DEFAULT_MAX_INDEX_BACKFILL_CATCH_UP_INTERVAL: u64 = 1_024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct IndexBackfillTailReport {
    pub from_ts: u64,
    pub to_ts: u64,
    pub consumed_commits: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
pub(crate) struct IndexBackfillSnapshot {
    pub table_object_id: u64,
    pub index_object_id: u64,
    pub backfill_read_ts: u64,
    pub last_tailed_ts: u64,
    pub max_catch_up_interval: u64,
}

#[derive(Debug)]
struct IndexBackfillState {
    last_tailed_ts: u64,
}

#[derive(Debug, Clone)]
pub(crate) struct IndexBackfillPlan {
    table_object_id: u64,
    index_object_id: u64,
    #[allow(dead_code)]
    backfill_read_ts: u64,
    max_catch_up_interval: u64,
    #[allow(dead_code)]
    lease: Arc<BackfillLease>,
    state: Arc<Mutex<IndexBackfillState>>,
}

impl IndexBackfillPlan {
    pub(crate) fn new(
        table_object_id: u64,
        index_object_id: u64,
        backfill_read_ts: u64,
        current_published_ts: u64,
        lease: BackfillLease,
    ) -> Self {
        Self {
            table_object_id,
            index_object_id,
            backfill_read_ts,
            max_catch_up_interval: DEFAULT_MAX_INDEX_BACKFILL_CATCH_UP_INTERVAL,
            lease: Arc::new(lease),
            state: Arc::new(Mutex::new(IndexBackfillState {
                last_tailed_ts: current_published_ts.max(backfill_read_ts),
            })),
        }
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn lease_info(&self) -> Result<RetentionLeaseInfo> {
        self.lease
            .info()
            .map_err(|err| paro_error::internal(format!("index backfill lease info: {err}")))
    }

    #[allow(dead_code)]
    pub(crate) fn snapshot(&self) -> Result<IndexBackfillSnapshot> {
        let state = self
            .state
            .lock()
            .map_err(|_| paro_error::internal("index backfill state poisoned"))?;
        Ok(IndexBackfillSnapshot {
            table_object_id: self.table_object_id,
            index_object_id: self.index_object_id,
            backfill_read_ts: self.backfill_read_ts,
            last_tailed_ts: state.last_tailed_ts,
            max_catch_up_interval: self.max_catch_up_interval,
        })
    }

    pub(crate) fn tail_committed_records_to(
        &self,
        target_ts: u64,
    ) -> Result<IndexBackfillTailReport> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| paro_error::internal("index backfill state poisoned"))?;
        let from_ts = state.last_tailed_ts;
        if target_ts > state.last_tailed_ts {
            state.last_tailed_ts = target_ts;
        }
        Ok(IndexBackfillTailReport {
            from_ts,
            to_ts: state.last_tailed_ts,
            consumed_commits: state.last_tailed_ts.saturating_sub(from_ts),
        })
    }

    pub(crate) fn bounded_final_catch_up(
        &self,
        publish_ts: u64,
        table: &TableCatalogEntry,
        index: &IndexCatalogEntry,
    ) -> Result<IndexBackfillTailReport> {
        self.validate_tokens(table, index)?;
        let mut state = self
            .state
            .lock()
            .map_err(|_| paro_error::internal("index backfill state poisoned"))?;
        let interval = publish_ts.saturating_sub(state.last_tailed_ts);
        if interval > self.max_catch_up_interval {
            return Err(paro_error::invalid_transaction_state(format!(
                "CREATE INDEX final catch-up interval {} exceeds limit {}",
                interval, self.max_catch_up_interval
            )));
        }
        let from_ts = state.last_tailed_ts;
        if publish_ts > state.last_tailed_ts {
            state.last_tailed_ts = publish_ts;
        }
        Ok(IndexBackfillTailReport {
            from_ts,
            to_ts: state.last_tailed_ts,
            consumed_commits: state.last_tailed_ts.saturating_sub(from_ts),
        })
    }

    fn validate_tokens(&self, table: &TableCatalogEntry, index: &IndexCatalogEntry) -> Result<()> {
        let table_object_id = table.base.base.object_id.raw();
        if table_object_id != self.table_object_id {
            return Err(paro_error::invalid_transaction_state(format!(
                "CREATE INDEX backfill table token mismatch: plan={} table={}",
                self.table_object_id, table_object_id
            )));
        }
        let index_object_id = index.base.base.object_id.raw();
        if index_object_id != self.index_object_id {
            return Err(paro_error::invalid_transaction_state(format!(
                "CREATE INDEX backfill index token mismatch: plan={} index={}",
                self.index_object_id, index_object_id
            )));
        }
        Ok(())
    }
}

#[inline]
pub(crate) fn lease_index_backfill(
    registry: &paro_transaction::RetentionRegistry,
    backfill_read_ts: u64,
    current_published_ts: u64,
) -> Result<BackfillLease> {
    registry
        .lease_backfill_range(
            CommitTs::new(backfill_read_ts),
            CommitTs::new(current_published_ts),
        )
        .map_err(|err| paro_error::internal(format!("lease index backfill: {err}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use paro_catalog::entry::{
        CatalogObjectId, ColumnDefinition, CreateIndexInfo, IndexCatalogEntry, LogicalIndex,
        TableCatalogEntry,
    };
    use paro_common::types::LogicalType;
    use paro_storage::table::table_factory::TableFactory;
    use paro_transaction::RetentionRegistry;

    fn table_entry_with_id(object_id: u64) -> Arc<TableCatalogEntry> {
        let storage = Arc::new(
            TableFactory::default()
                .create_table(&[LogicalType::Integer])
                .unwrap(),
        );
        Arc::new(TableCatalogEntry::new(
            "memory".to_string(),
            "main".to_string(),
            "t".to_string(),
            vec![ColumnDefinition::new(
                "id".to_string(),
                LogicalType::Integer,
            )],
            storage,
            CatalogObjectId::from_raw(object_id),
            0,
        ))
    }

    fn table_entry() -> Arc<TableCatalogEntry> {
        table_entry_with_id(1)
    }

    fn index_entry(table: &TableCatalogEntry) -> Arc<IndexCatalogEntry> {
        let info = CreateIndexInfo::new(
            "main".to_string(),
            "t".to_string(),
            "idx_t_id".to_string(),
            vec![LogicalIndex::new(0)],
            vec![LogicalType::Integer],
        );
        Arc::new(IndexCatalogEntry::new(
            info,
            table.base.base.object_id.raw(),
            10_001,
            "memory".to_string(),
            CatalogObjectId::from_raw(10_002),
        ))
    }

    #[test]
    fn backfill_plan_pins_range_and_bounds_final_catch_up() {
        let registry = RetentionRegistry::with_capacity(1, 2);
        let table = table_entry();
        let index = index_entry(&table);
        let lease = lease_index_backfill(&registry, 10, 12).unwrap();
        let plan = IndexBackfillPlan::new(
            table.base.base.object_id.raw(),
            index.base.base.object_id.raw(),
            10,
            12,
            lease,
        );

        assert_eq!(
            plan.lease_info().unwrap().commit_ts_floor,
            Some(CommitTs::new(10))
        );
        assert_eq!(
            plan.lease_info().unwrap().commit_ts_ceiling,
            Some(CommitTs::new(12))
        );
        assert_eq!(
            plan.tail_committed_records_to(17).unwrap(),
            IndexBackfillTailReport {
                from_ts: 12,
                to_ts: 17,
                consumed_commits: 5,
            }
        );
        assert_eq!(
            plan.bounded_final_catch_up(20, table.as_ref(), index.as_ref())
                .unwrap()
                .consumed_commits,
            3
        );
        assert!(plan
            .bounded_final_catch_up(
                20 + DEFAULT_MAX_INDEX_BACKFILL_CATCH_UP_INTERVAL + 1,
                table.as_ref(),
                index.as_ref()
            )
            .is_err());
        let other_table = table_entry_with_id(2);
        assert!(plan
            .bounded_final_catch_up(21, other_table.as_ref(), index.as_ref())
            .is_err());
    }
}
