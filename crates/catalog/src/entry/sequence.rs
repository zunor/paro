// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Sequence Catalog Entry
//!
//! This module defines SequenceCatalogEntry for sequence metadata.

use super::catalog_entry::{
    allocate_object_id, AlterInfo, CatalogEntry, CatalogObjectId, CatalogType, CreateInfo,
    DependencyList, InCatalogEntry, OnCreateConflict, SchemaEntryMeta, StandardEntry,
};
use paro_common::error::{self as paro_error, Result};
use std::collections::HashMap;
use std::io::{Cursor, Read, Write};
use std::sync::{Arc, LazyLock, RwLock, Weak};

// --- Constants ---

pub const DEFAULT_INCREMENT: i64 = 1;
pub const DEFAULT_MIN_VALUE: i64 = 1;
pub const DEFAULT_MAX_VALUE: i64 = i64::MAX;
pub const DEFAULT_START_VALUE: i64 = 1;

// --- SequenceData ---

/// Sequence data - the mutable state of a sequence.
///
#[derive(Debug, Clone)]
pub struct SequenceData {
    /// The amount of times the sequence has been used
    pub usage_count: u64,
    /// The sequence counter (next value to return)
    pub counter: i64,
    /// The most recently returned value
    pub last_value: i64,
    /// The increment value
    pub increment: i64,
    /// The start value of the sequence
    pub start_value: i64,
    /// The minimum value of the sequence
    pub min_value: i64,
    /// The maximum value of the sequence
    pub max_value: i64,
    /// Whether or not the sequence cycles
    pub cycle: bool,
}

impl SequenceData {
    pub fn new(info: &CreateSequenceInfo) -> Self {
        Self {
            usage_count: 0,
            counter: info.start_value,
            last_value: info.start_value,
            increment: info.increment,
            start_value: info.start_value,
            min_value: info.min_value,
            max_value: info.max_value,
            cycle: info.cycle,
        }
    }
}

// --- CreateSequenceInfo ---

/// Information needed to create a sequence.
///
#[derive(Debug, Clone)]
pub struct CreateSequenceInfo {
    /// Catalog name
    pub catalog: String,
    /// Schema name
    pub schema: String,
    /// Sequence name
    pub name: String,
    /// The increment value
    pub increment: i64,
    /// The minimum value
    pub min_value: i64,
    /// The maximum value
    pub max_value: i64,
    /// The start value
    pub start_value: i64,
    /// Whether the sequence cycles
    pub cycle: bool,
    /// On conflict behavior
    pub on_conflict: OnCreateConflict,
    /// Whether this is temporary
    pub temporary: bool,
    /// Dependencies
    pub dependencies: DependencyList,
}

impl CreateSequenceInfo {
    pub fn new(schema: String, name: String) -> Self {
        Self {
            catalog: String::new(),
            schema,
            name,
            increment: DEFAULT_INCREMENT,
            min_value: DEFAULT_MIN_VALUE,
            max_value: DEFAULT_MAX_VALUE,
            start_value: DEFAULT_START_VALUE,
            cycle: false,
            on_conflict: OnCreateConflict::ErrorOnConflict,
            temporary: false,
            dependencies: DependencyList::new(),
        }
    }

    pub fn with_catalog(mut self, catalog: String) -> Self {
        self.catalog = catalog;
        self
    }

    pub fn with_increment(mut self, increment: i64) -> Self {
        self.increment = increment;
        self
    }

    pub fn with_min_value(mut self, min_value: i64) -> Self {
        self.min_value = min_value;
        self
    }

    pub fn with_max_value(mut self, max_value: i64) -> Self {
        self.max_value = max_value;
        self
    }

    pub fn with_start_value(mut self, start_value: i64) -> Self {
        self.start_value = start_value;
        self
    }

    pub fn with_cycle(mut self) -> Self {
        self.cycle = true;
        self
    }

    pub fn with_temporary(mut self) -> Self {
        self.temporary = true;
        self
    }

    pub fn with_if_not_exists(mut self) -> Self {
        self.on_conflict = OnCreateConflict::IgnoreOnConflict;
        self
    }

    pub fn with_or_replace(mut self) -> Self {
        self.on_conflict = OnCreateConflict::ReplaceOnConflict;
        self
    }

    pub fn validate(&self) -> Result<()> {
        if self.increment == 0 {
            return Err(paro_error::invalid_input(
                "sequence increment cannot be zero",
            ));
        }
        if self.min_value > self.max_value {
            return Err(paro_error::invalid_input(
                "sequence min_value cannot be greater than max_value",
            ));
        }
        if self.start_value < self.min_value || self.start_value > self.max_value {
            return Err(paro_error::invalid_input(
                "sequence start_value must be between min_value and max_value",
            ));
        }
        Ok(())
    }
}

// --- SequenceCatalogEntry ---

/// Sequence catalog entry - metadata for a sequence.
///
#[derive(Debug)]
pub struct SequenceCatalogEntry {
    /// Standard entry base
    pub base: SchemaEntryMeta,
    /// Sequence data (protected by RwLock for thread-safe access)
    data: RwLock<SequenceData>,
}

impl SequenceCatalogEntry {
    /// Create a new sequence catalog entry from CreateSequenceInfo
    pub fn new(info: CreateSequenceInfo, timestamp: u64, catalog: String) -> Result<Self> {
        Self::with_object_id(info, timestamp, catalog, allocate_object_id())
    }

    pub fn with_object_id(
        info: CreateSequenceInfo,
        timestamp: u64,
        catalog: String,
        object_id: CatalogObjectId,
    ) -> Result<Self> {
        info.validate()?;

        let mut base = SchemaEntryMeta::new(
            CatalogType::Sequence,
            catalog,
            info.schema.clone(),
            info.name.clone(),
            object_id,
            timestamp,
        );
        base.base.temporary = info.temporary;

        let data = SequenceData::new(&info);
        base.set_dependencies(info.dependencies);

        Ok(Self {
            base,
            data: RwLock::new(data),
        })
    }

    /// Get a copy of the sequence data
    pub fn get_data(&self) -> SequenceData {
        self.data
            .read()
            .map(|d| d.clone())
            .unwrap_or_else(|_| SequenceData {
                usage_count: 0,
                counter: 0,
                last_value: 0,
                increment: 1,
                start_value: 1,
                min_value: 1,
                max_value: i64::MAX,
                cycle: false,
            })
    }

    /// Get the current value of the sequence
    pub fn current_value(&self) -> Result<i64> {
        let data = self
            .data
            .read()
            .map_err(|_| paro_error::internal("lock poisoned"))?;
        if data.usage_count == 0 {
            return Err(paro_error::sequence_generator_error(
                "currval: sequence is not yet defined in this session",
            ));
        }
        Ok(data.last_value)
    }

    /// Get the next value of the sequence
    pub fn next_value(&self) -> Result<i64> {
        let mut data = self
            .data
            .write()
            .map_err(|_| paro_error::internal("lock poisoned"))?;

        let result = data.counter;
        let (new_counter, overflow) = data.counter.overflowing_add(data.increment);

        if data.cycle {
            if overflow {
                data.counter = if data.increment < 0 {
                    data.max_value
                } else {
                    data.min_value
                };
            } else if new_counter < data.min_value {
                data.counter = data.max_value;
            } else if new_counter > data.max_value {
                data.counter = data.min_value;
            } else {
                data.counter = new_counter;
            }
        } else {
            if result < data.min_value || (overflow && data.increment < 0) {
                return Err(paro_error::sequence_generator_error(format!(
                    "nextval: reached minimum value of sequence \"{}\" ({})",
                    self.base.base.name, data.min_value
                )));
            }
            if result > data.max_value || (overflow && data.increment > 0) {
                return Err(paro_error::sequence_generator_error(format!(
                    "nextval: reached maximum value of sequence \"{}\" ({})",
                    self.base.base.name, data.max_value
                )));
            }
            data.counter = new_counter;
        }

        data.last_value = result;
        data.usage_count += 1;

        Ok(result)
    }

    /// Set the sequence value (for ALTER SEQUENCE ... RESTART)
    pub fn set_value(&self, value: i64) -> Result<()> {
        let data = self
            .data
            .read()
            .map_err(|_| paro_error::internal("lock poisoned"))?;
        if value < data.min_value || value > data.max_value {
            return Err(paro_error::invalid_input(format!(
                "sequence value {} is out of range [{}, {}]",
                value, data.min_value, data.max_value
            )));
        }
        drop(data);

        let mut data = self
            .data
            .write()
            .map_err(|_| paro_error::internal("lock poisoned"))?;
        data.counter = value;
        Ok(())
    }

    /// Replay a sequence value (for recovery)
    pub fn replay_value(&self, usage_count: u64, counter: i64) {
        if let Ok(mut data) = self.data.write() {
            if usage_count > data.usage_count {
                data.usage_count = usage_count;
                data.counter = counter;
            }
        }
    }

    pub fn get_increment(&self) -> i64 {
        self.data.read().map(|d| d.increment).unwrap_or(1)
    }

    pub fn get_min_value(&self) -> i64 {
        self.data.read().map(|d| d.min_value).unwrap_or(1)
    }

    pub fn get_max_value(&self) -> i64 {
        self.data.read().map(|d| d.max_value).unwrap_or(i64::MAX)
    }

    pub fn get_start_value(&self) -> i64 {
        self.data.read().map(|d| d.start_value).unwrap_or(1)
    }

    pub fn is_cycle(&self) -> bool {
        self.data.read().map(|d| d.cycle).unwrap_or(false)
    }

    /// Convert to SQL CREATE SEQUENCE statement
    pub fn to_sql(&self) -> String {
        let data = self.get_data();

        let mut sql = String::new();
        sql.push_str("CREATE ");

        if self.base.base.temporary {
            sql.push_str("TEMPORARY ");
        }

        sql.push_str("SEQUENCE ");
        sql.push_str(&self.base.schema_name);
        sql.push('.');
        sql.push_str(&self.base.base.name);

        sql.push_str(" INCREMENT BY ");
        sql.push_str(&data.increment.to_string());

        sql.push_str(" MINVALUE ");
        sql.push_str(&data.min_value.to_string());

        sql.push_str(" MAXVALUE ");
        sql.push_str(&data.max_value.to_string());

        sql.push_str(" START ");
        sql.push_str(&data.counter.to_string());

        sql.push(' ');
        sql.push_str(if data.cycle { "CYCLE" } else { "NO CYCLE" });

        sql.push(';');
        sql
    }

    /// Serialize the sequence entry to bytes
    pub fn serialize_to_bytes(&self) -> Result<Vec<u8>> {
        let data = self.get_data();
        let mut buffer = Vec::new();

        buffer.write_all(&self.base.base.object_id.raw().to_le_bytes())?;
        buffer.write_all(&self.base.base.timestamp().to_le_bytes())?;

        let name_bytes = self.base.base.name.as_bytes();
        buffer.write_all(&(name_bytes.len() as u32).to_le_bytes())?;
        buffer.write_all(name_bytes)?;

        let schema_bytes = self.base.schema_name.as_bytes();
        buffer.write_all(&(schema_bytes.len() as u32).to_le_bytes())?;
        buffer.write_all(schema_bytes)?;

        buffer.write_all(&data.usage_count.to_le_bytes())?;
        buffer.write_all(&data.counter.to_le_bytes())?;
        buffer.write_all(&data.last_value.to_le_bytes())?;
        buffer.write_all(&data.increment.to_le_bytes())?;
        buffer.write_all(&data.start_value.to_le_bytes())?;
        buffer.write_all(&data.min_value.to_le_bytes())?;
        buffer.write_all(&data.max_value.to_le_bytes())?;
        buffer.write_all(&[if data.cycle { 1u8 } else { 0u8 }])?;
        buffer.write_all(&[if self.base.base.temporary { 1u8 } else { 0u8 }])?;

        Ok(buffer)
    }

    /// Deserialize a sequence entry from bytes
    pub fn deserialize(bytes: &[u8], catalog: String) -> Result<Self> {
        let mut cursor = Cursor::new(bytes);

        let mut oid_buf = [0u8; 8];
        cursor.read_exact(&mut oid_buf)?;
        let oid = u64::from_le_bytes(oid_buf);

        let mut ts_buf = [0u8; 8];
        cursor.read_exact(&mut ts_buf)?;
        let timestamp = u64::from_le_bytes(ts_buf);

        let mut len_buf = [0u8; 4];
        cursor.read_exact(&mut len_buf)?;
        let name_len = u32::from_le_bytes(len_buf) as usize;
        let mut name_bytes = vec![0u8; name_len];
        cursor.read_exact(&mut name_bytes)?;
        let sequence_name = String::from_utf8(name_bytes)
            .map_err(|e| paro_error::internal(format!("Invalid UTF-8: {}", e)))?;

        cursor.read_exact(&mut len_buf)?;
        let schema_len = u32::from_le_bytes(len_buf) as usize;
        let mut schema_bytes = vec![0u8; schema_len];
        cursor.read_exact(&mut schema_bytes)?;
        let schema_name = String::from_utf8(schema_bytes)
            .map_err(|e| paro_error::internal(format!("Invalid UTF-8: {}", e)))?;

        cursor.read_exact(&mut ts_buf)?;
        let usage_count = u64::from_le_bytes(ts_buf);

        let mut i64_buf = [0u8; 8];
        cursor.read_exact(&mut i64_buf)?;
        let counter = i64::from_le_bytes(i64_buf);

        cursor.read_exact(&mut i64_buf)?;
        let last_value = i64::from_le_bytes(i64_buf);

        cursor.read_exact(&mut i64_buf)?;
        let increment = i64::from_le_bytes(i64_buf);

        cursor.read_exact(&mut i64_buf)?;
        let start_value = i64::from_le_bytes(i64_buf);

        cursor.read_exact(&mut i64_buf)?;
        let min_value = i64::from_le_bytes(i64_buf);

        cursor.read_exact(&mut i64_buf)?;
        let max_value = i64::from_le_bytes(i64_buf);

        let mut byte_buf = [0u8; 1];
        cursor.read_exact(&mut byte_buf)?;
        let cycle = byte_buf[0] == 1;

        cursor.read_exact(&mut byte_buf)?;
        let temporary = byte_buf[0] == 1;

        let mut base = SchemaEntryMeta::new(
            CatalogType::Sequence,
            catalog,
            schema_name,
            sequence_name,
            CatalogObjectId::from_raw(oid),
            timestamp,
        );
        base.base.temporary = temporary;

        let data = SequenceData {
            usage_count,
            counter,
            last_value,
            increment,
            start_value,
            min_value,
            max_value,
            cycle,
        };

        Ok(Self {
            base,
            data: RwLock::new(data),
        })
    }
}

// --- CatalogEntry trait implementation ---

impl CatalogEntry for SequenceCatalogEntry {
    fn object_id(&self) -> CatalogObjectId {
        self.base.base.object_id
    }

    fn name(&self) -> &str {
        &self.base.base.name
    }

    fn entry_type(&self) -> CatalogType {
        CatalogType::Sequence
    }

    fn catalog_name(&self) -> &str {
        &self.base.base.catalog
    }

    fn timestamp(&self) -> u64 {
        self.base.base.timestamp()
    }

    fn set_timestamp(&self, ts: u64) {
        self.base.base.set_timestamp(ts);
    }

    fn is_deleted(&self) -> bool {
        self.base.base.is_deleted()
    }

    fn set_deleted(&self, deleted: bool) {
        self.base.base.set_deleted(deleted);
    }

    fn child(&self) -> Option<Arc<dyn CatalogEntry>> {
        self.base.base.child()
    }

    fn set_child(&self, child: Option<Arc<dyn CatalogEntry>>) {
        self.base.base.set_child(child);
    }

    fn parent(&self) -> Option<Arc<dyn CatalogEntry>> {
        self.base.base.parent()
    }

    fn set_parent(&self, parent: Option<Weak<dyn CatalogEntry>>) {
        self.base.base.set_parent(parent);
    }

    fn is_temporary(&self) -> bool {
        self.base.base.temporary
    }

    fn is_internal(&self) -> bool {
        self.base.base.internal
    }

    fn comment(&self) -> Option<&str> {
        None
    }

    fn set_comment(&self, comment: Option<String>) {
        self.base.base.set_comment(comment);
    }

    fn tags(&self) -> &HashMap<String, String> {
        static EMPTY: LazyLock<HashMap<String, String>> = LazyLock::new(HashMap::new);
        &EMPTY
    }

    fn set_tags(&self, tags: HashMap<String, String>) {
        self.base.base.set_tags(tags);
    }

    fn alter(&self, _info: &AlterInfo) -> Result<Arc<dyn CatalogEntry>> {
        Err(paro_error::not_implemented("ALTER SEQUENCE"))
    }

    fn undo_alter(&self, _info: &AlterInfo) -> Result<()> {
        Ok(())
    }

    fn rollback(&self, _prev_entry: &dyn CatalogEntry) -> Result<()> {
        Ok(())
    }

    fn copy(&self) -> Result<Arc<dyn CatalogEntry>> {
        Err(paro_error::not_implemented("SEQUENCE copy"))
    }

    fn get_info(&self) -> Result<CreateInfo> {
        let mut info = CreateInfo::new(
            CatalogType::Sequence,
            self.base.base.catalog.clone(),
            self.base.schema_name.clone(),
            self.base.base.name.clone(),
        );
        info.temporary = self.base.base.temporary;
        info.sql = Some(self.to_sql());
        Ok(info)
    }

    fn set_as_root(&self) {}

    fn to_sql(&self) -> String {
        self.to_sql()
    }

    fn serialize(&self, writer: &mut dyn std::io::Write) -> Result<()> {
        self.base.base.serialize(writer)?;
        Ok(())
    }
}

// --- StandardEntry trait implementation ---

impl StandardEntry for SequenceCatalogEntry {
    fn schema_name(&self) -> &str {
        &self.base.schema_name
    }

    fn dependencies(&self) -> &DependencyList {
        static EMPTY: LazyLock<DependencyList> = LazyLock::new(DependencyList::new);
        &EMPTY
    }

    fn set_dependencies(&self, dependencies: DependencyList) {
        self.base.set_dependencies(dependencies);
    }
}

// --- InCatalogEntry trait implementation ---

impl InCatalogEntry for SequenceCatalogEntry {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_sequence_info() {
        let info = CreateSequenceInfo::new("public".to_string(), "my_seq".to_string());
        assert_eq!(info.schema, "public");
        assert_eq!(info.name, "my_seq");
        assert_eq!(info.increment, 1);
    }

    #[test]
    fn test_sequence_catalog_entry() {
        let info = CreateSequenceInfo::new("public".to_string(), "test_seq".to_string());
        let entry = SequenceCatalogEntry::new(info, 100, "main".to_string()).unwrap();

        assert_eq!(entry.name(), "test_seq");
        assert_eq!(entry.schema_name(), "public");
        assert_eq!(entry.entry_type(), CatalogType::Sequence);
    }

    #[test]
    fn test_sequence_next_value() {
        let info = CreateSequenceInfo::new("public".to_string(), "seq".to_string());
        let entry = SequenceCatalogEntry::new(info, 100, "main".to_string()).unwrap();

        assert_eq!(entry.next_value().unwrap(), 1);
        assert_eq!(entry.next_value().unwrap(), 2);
        assert_eq!(entry.next_value().unwrap(), 3);
    }

    #[test]
    fn test_sequence_roundtrip_preserves_object_id() {
        let info = CreateSequenceInfo::new("public".to_string(), "roundtrip_seq".to_string())
            .with_start_value(5)
            .with_increment(2);
        let entry = SequenceCatalogEntry::new(info, 100, "main".to_string()).unwrap();

        let bytes = entry.serialize_to_bytes().unwrap();
        let restored = SequenceCatalogEntry::deserialize(&bytes, "main".to_string()).unwrap();

        assert_eq!(restored.object_id(), entry.object_id());
        assert_eq!(restored.name(), "roundtrip_seq");
        assert_eq!(restored.get_data().start_value, 5);
    }
}
