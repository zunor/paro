// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use paro_common::error::{self as paro_error, Result};
use paro_transaction::DatabaseId;
use std::sync::Arc;
use std::sync::Mutex;

use crate::WriteClass;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriteDatabase {
    id: DatabaseId,
    name: Arc<str>,
}

impl WriteDatabase {
    pub fn new(id: DatabaseId, name: impl AsRef<str>) -> Self {
        Self {
            id,
            name: Arc::from(name.as_ref()),
        }
    }

    #[inline]
    pub const fn id(&self) -> DatabaseId {
        self.id
    }

    #[inline]
    pub fn name(&self) -> &str {
        &self.name
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct WriteGuardMark {
    class: WriteClass,
    write_database: Option<WriteDatabase>,
}

#[derive(Debug, Clone, Default)]
struct WriteState {
    class: WriteClass,
    transaction_database: Option<WriteDatabase>,
    write_database: Option<WriteDatabase>,
}

#[derive(Debug, Default)]
pub struct WriteGuard {
    state: Mutex<WriteState>,
}

impl WriteGuard {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn set_transaction_database(&self, database_id: DatabaseId, database_name: &str) {
        if let Ok(mut state) = self.state.lock() {
            state.transaction_database = Some(WriteDatabase::new(database_id, database_name));
            state.write_database = None;
            state.class = WriteClass::Clean;
        }
    }

    pub fn bind_database(&self, database_id: DatabaseId, database_name: &str) -> Result<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| paro_error::internal("txn write state poisoned"))?;
        Self::bind_database_locked(&mut state, database_id, database_name)
    }

    pub fn begin_dml_write(&self) -> Result<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| paro_error::internal("txn write state poisoned"))?;
        Self::ensure_database_bound_locked(&state, "DML")?;
        match state.class {
            WriteClass::Clean | WriteClass::HasDml => {
                state.class = WriteClass::HasDml;
                Ok(())
            }
            WriteClass::HasDdl | WriteClass::HasDmlAndDdl => {
                state.class = WriteClass::HasDmlAndDdl;
                Ok(())
            }
        }
    }

    pub fn begin_dml_write_in_database(
        &self,
        database_id: DatabaseId,
        database_name: &str,
    ) -> Result<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| paro_error::internal("txn write state poisoned"))?;
        Self::bind_database_locked(&mut state, database_id, database_name)?;
        match state.class {
            WriteClass::Clean | WriteClass::HasDml => {
                state.class = WriteClass::HasDml;
                Ok(())
            }
            WriteClass::HasDdl | WriteClass::HasDmlAndDdl => {
                state.class = WriteClass::HasDmlAndDdl;
                Ok(())
            }
        }
    }

    pub fn begin_object_ddl(&self) -> Result<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| paro_error::internal("txn write state poisoned"))?;
        Self::ensure_database_bound_locked(&state, "DDL")?;
        match state.class {
            WriteClass::Clean | WriteClass::HasDdl => {
                state.class = WriteClass::HasDdl;
                Ok(())
            }
            WriteClass::HasDml | WriteClass::HasDmlAndDdl => {
                state.class = WriteClass::HasDmlAndDdl;
                Ok(())
            }
        }
    }

    pub fn begin_object_ddl_in_database(
        &self,
        database_id: DatabaseId,
        database_name: &str,
    ) -> Result<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| paro_error::internal("txn write state poisoned"))?;
        Self::bind_database_locked(&mut state, database_id, database_name)?;
        match state.class {
            WriteClass::Clean | WriteClass::HasDdl => {
                state.class = WriteClass::HasDdl;
                Ok(())
            }
            WriteClass::HasDml | WriteClass::HasDmlAndDdl => {
                state.class = WriteClass::HasDmlAndDdl;
                Ok(())
            }
        }
    }

    pub fn class(&self) -> WriteClass {
        self.state
            .lock()
            .map(|guard| guard.class)
            .unwrap_or(WriteClass::HasDmlAndDdl)
    }

    pub fn write_database(&self) -> Option<WriteDatabase> {
        self.state
            .lock()
            .ok()
            .and_then(|state| state.write_database.clone())
    }

    pub fn mark(&self) -> WriteGuardMark {
        self.state
            .lock()
            .map(|state| WriteGuardMark {
                class: state.class,
                write_database: state.write_database.clone(),
            })
            .unwrap_or_default()
    }

    pub fn restore(&self, mark: WriteGuardMark) {
        if let Ok(mut state) = self.state.lock() {
            state.class = mark.class;
            state.write_database = mark.write_database;
        }
    }

    pub fn reset(&self) {
        if let Ok(mut state) = self.state.lock() {
            *state = WriteState::default();
        }
    }

    fn bind_database_locked(
        state: &mut WriteState,
        database_id: DatabaseId,
        database_name: &str,
    ) -> Result<()> {
        let target = WriteDatabase::new(database_id, database_name);
        if let Some(home) = &state.transaction_database {
            if home.id != target.id {
                return Err(cross_database_write_error(home, &target));
            }
        }
        if let Some(bound) = &state.write_database {
            if bound.id != target.id {
                return Err(cross_database_write_error(bound, &target));
            }
            return Ok(());
        }
        state.write_database = Some(target);
        Ok(())
    }

    fn ensure_database_bound_locked(state: &WriteState, write_kind: &str) -> Result<()> {
        if state.write_database.is_some() {
            return Ok(());
        }
        Err(paro_error::invalid_transaction_state(format!(
            "{write_kind} write must bind a target database before entering the transaction write set"
        )))
    }
}

fn cross_database_write_error(
    existing: &WriteDatabase,
    attempted: &WriteDatabase,
) -> paro_error::ParoError {
    paro_error::invalid_transaction_state(format!(
        "cross-database write transaction is not supported: transaction is bound to database \"{}\" (id={}), attempted database \"{}\" (id={})",
        display_database_name(existing.name()),
        existing.id(),
        display_database_name(attempted.name()),
        attempted.id()
    ))
}

fn display_database_name(name: &str) -> &str {
    if name.is_empty() {
        "<unknown>"
    } else {
        name
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_writes_outside_transaction_database() {
        let guard = WriteGuard::new();
        guard.set_transaction_database(DatabaseId::new(1), "main");

        let err = guard
            .begin_dml_write_in_database(DatabaseId::new(2), "analytics")
            .unwrap_err();

        assert!(err.to_string().contains("cross-database write"));
        assert_eq!(guard.class(), WriteClass::Clean);
        assert!(guard.write_database().is_none());
    }

    #[test]
    fn savepoint_restore_unbinds_write_database() {
        let guard = WriteGuard::new();
        guard.set_transaction_database(DatabaseId::new(1), "main");
        let mark = guard.mark();

        guard
            .begin_object_ddl_in_database(DatabaseId::new(1), "main")
            .unwrap();
        assert_eq!(
            guard.write_database().map(|database| database.id()),
            Some(DatabaseId::new(1))
        );

        guard.restore(mark);

        assert_eq!(guard.class(), WriteClass::Clean);
        assert!(guard.write_database().is_none());
        guard
            .begin_dml_write_in_database(DatabaseId::new(1), "main")
            .unwrap();
    }
}
