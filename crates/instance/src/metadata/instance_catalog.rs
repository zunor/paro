// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::time::{SystemTime, UNIX_EPOCH};

pub const INSTANCE_CATALOG_FORMAT_VERSION: u16 = 1;
pub const FIRST_MANAGED_DATABASE_ID: u64 = 1;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct InstanceCatalog {
    pub format_version: u16,
    pub next_database_id: u64,
    pub default_database_id: Option<u64>,
    pub databases: Vec<DatabaseRecord>,
    pub last_updated_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DatabaseRecord {
    pub database_id: u64,
    pub name: String,
    pub state: DatabaseRecordState,
    pub storage_dir: String,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DatabaseRecordState {
    Provisioning,
    Ready,
    Offline,
    Broken,
    Dropping,
}

impl DatabaseRecordState {
    pub fn allows_runtime_open(self) -> bool {
        matches!(self, Self::Ready)
    }

    pub fn can_drop(self) -> bool {
        matches!(self, Self::Ready | Self::Offline | Self::Broken)
    }

    pub fn can_rename(self) -> bool {
        matches!(self, Self::Ready | Self::Offline | Self::Broken)
    }
}

impl Default for InstanceCatalog {
    fn default() -> Self {
        Self::new_empty()
    }
}

impl DatabaseRecord {
    pub fn new(
        database_id: u64,
        name: String,
        state: DatabaseRecordState,
        storage_dir: String,
    ) -> Self {
        Self {
            database_id,
            name,
            state,
            storage_dir,
            last_error: None,
        }
    }
}

impl InstanceCatalog {
    pub fn new_empty() -> Self {
        Self {
            format_version: INSTANCE_CATALOG_FORMAT_VERSION,
            next_database_id: FIRST_MANAGED_DATABASE_ID,
            default_database_id: None,
            databases: Vec::new(),
            last_updated_ms: current_timestamp_ms(),
        }
    }

    pub fn find_database_by_name(&self, name: &str) -> Option<&DatabaseRecord> {
        self.databases
            .iter()
            .find(|record| record.name.eq_ignore_ascii_case(name))
    }

    pub fn find_database_by_id(&self, database_id: u64) -> Option<&DatabaseRecord> {
        self.databases
            .iter()
            .find(|record| record.database_id == database_id)
    }

    pub fn find_database_mut_by_id(&mut self, database_id: u64) -> Option<&mut DatabaseRecord> {
        self.databases
            .iter_mut()
            .find(|record| record.database_id == database_id)
    }

    pub fn find_database_mut_by_name(&mut self, name: &str) -> Option<&mut DatabaseRecord> {
        self.databases
            .iter_mut()
            .find(|record| record.name.eq_ignore_ascii_case(name))
    }

    pub fn database_name_by_id(&self, database_id: u64) -> Option<&str> {
        self.find_database_by_id(database_id)
            .map(|record| record.name.as_str())
    }

    pub fn has_transient_records(&self) -> bool {
        self.databases.iter().any(|record| {
            matches!(
                record.state,
                DatabaseRecordState::Provisioning | DatabaseRecordState::Dropping
            )
        })
    }

    pub fn allocate_database(
        &mut self,
        name: String,
        storage_dir: String,
    ) -> anyhow::Result<DatabaseRecord> {
        if self.find_database_by_name(&name).is_some() {
            anyhow::bail!("database \"{}\" already exists in instance catalog", name);
        }

        let database_id = self.next_database_id;
        if database_id < FIRST_MANAGED_DATABASE_ID {
            anyhow::bail!(
                "invalid next_database_id {} in instance catalog",
                self.next_database_id
            );
        }
        self.next_database_id = self
            .next_database_id
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("database_id overflow at {}", database_id))?;

        let record = DatabaseRecord::new(
            database_id,
            name,
            DatabaseRecordState::Provisioning,
            storage_dir,
        );
        self.databases.push(record.clone());
        self.touch();
        Ok(record)
    }

    pub fn remove_database_by_id(&mut self, database_id: u64) -> Option<DatabaseRecord> {
        let index = self
            .databases
            .iter()
            .position(|record| record.database_id == database_id)?;
        let removed = self.databases.remove(index);
        if self.default_database_id == Some(database_id) {
            self.default_database_id = None;
        }
        self.touch();
        Some(removed)
    }

    pub fn set_default_database(&mut self, database_id: Option<u64>) -> anyhow::Result<()> {
        if let Some(id) = database_id {
            if self.find_database_by_id(id).is_none() {
                anyhow::bail!("default database id {} does not exist", id);
            }
        }
        self.default_database_id = database_id;
        self.touch();
        Ok(())
    }

    pub fn rename_database(&mut self, database_id: u64, new_name: String) -> anyhow::Result<()> {
        if let Some(existing) = self.find_database_by_name(&new_name) {
            if existing.database_id != database_id {
                anyhow::bail!(
                    "database \"{}\" already exists in instance catalog",
                    new_name
                );
            }
        }

        let record = self.find_database_mut_by_id(database_id).ok_or_else(|| {
            anyhow::anyhow!("database_id {} not found in instance catalog", database_id)
        })?;
        if record.name.eq_ignore_ascii_case(&new_name) {
            return Ok(());
        }

        record.name = new_name;
        record.last_error = None;
        self.touch();
        Ok(())
    }

    pub fn touch(&mut self) {
        self.last_updated_ms = current_timestamp_ms();
    }

    pub fn validate(&self) -> anyhow::Result<()> {
        if self.format_version != INSTANCE_CATALOG_FORMAT_VERSION {
            anyhow::bail!(
                "unsupported instance catalog format version {}",
                self.format_version
            );
        }
        if self.next_database_id < FIRST_MANAGED_DATABASE_ID {
            anyhow::bail!(
                "next_database_id {} is below the managed database range",
                self.next_database_id
            );
        }

        let mut ids = HashSet::new();
        let mut names = HashSet::new();
        let mut storage_dirs = HashSet::new();
        let mut max_database_id = 0u64;

        for record in &self.databases {
            if record.database_id < FIRST_MANAGED_DATABASE_ID {
                anyhow::bail!(
                    "database_id {} is reserved and cannot appear in instance catalog",
                    record.database_id
                );
            }
            if record.name.is_empty() {
                anyhow::bail!("instance catalog contains database with empty name");
            }
            if record.storage_dir.is_empty() {
                anyhow::bail!(
                    "instance catalog contains database \"{}\" with empty storage_dir",
                    record.name
                );
            }
            if !ids.insert(record.database_id) {
                anyhow::bail!(
                    "duplicate database_id {} in instance catalog",
                    record.database_id
                );
            }
            let lower_name = record.name.to_lowercase();
            if !names.insert(lower_name.clone()) {
                anyhow::bail!(
                    "duplicate database name \"{}\" in instance catalog",
                    record.name
                );
            }
            if record.storage_dir != ":memory:" && !storage_dirs.insert(record.storage_dir.clone())
            {
                anyhow::bail!(
                    "duplicate storage_dir \"{}\" in instance catalog",
                    record.storage_dir
                );
            }
            max_database_id = max_database_id.max(record.database_id);
        }

        if let Some(default_database_id) = self.default_database_id {
            if !ids.contains(&default_database_id) {
                anyhow::bail!(
                    "default_database_id {} does not exist in instance catalog",
                    default_database_id
                );
            }
        }

        if max_database_id >= self.next_database_id {
            anyhow::bail!(
                "next_database_id {} must be greater than existing database_id {}",
                self.next_database_id,
                max_database_id
            );
        }

        Ok(())
    }
}

fn current_timestamp_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or(0)
        .min(i64::MAX as u128) as i64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn allocates_managed_database_ids_from_one() {
        let mut catalog = InstanceCatalog::new_empty();

        let first = catalog
            .allocate_database("postgres".to_string(), "databases/db-1".to_string())
            .unwrap();
        let second = catalog
            .allocate_database("analytics".to_string(), "databases/db-2".to_string())
            .unwrap();

        assert_eq!(first.database_id, 1);
        assert_eq!(second.database_id, 2);
        assert_eq!(catalog.next_database_id, 3);
    }

    #[test]
    fn validate_rejects_duplicate_names_case_insensitively() {
        let catalog = InstanceCatalog {
            format_version: INSTANCE_CATALOG_FORMAT_VERSION,
            next_database_id: 3,
            default_database_id: Some(1),
            databases: vec![
                DatabaseRecord::new(
                    1,
                    "postgres".to_string(),
                    DatabaseRecordState::Ready,
                    "databases/db-1".to_string(),
                ),
                DatabaseRecord::new(
                    2,
                    "POSTGRES".to_string(),
                    DatabaseRecordState::Ready,
                    "databases/db-2".to_string(),
                ),
            ],
            last_updated_ms: 0,
        };

        let err = catalog.validate().expect_err("duplicate names should fail");
        assert!(err.to_string().contains("duplicate database name"));
    }
}
