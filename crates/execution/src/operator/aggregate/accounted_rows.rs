// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Accounted row collections for aggregate DISTINCT / ORDER BY modifiers.

use std::hash::{Hash, Hasher};
use std::mem::size_of;
use std::ops::Deref;
use std::sync::Arc;

use paro_common::memory::{
    AccountedHashSet, AccountedVec, MemoryAccountingClass, MemoryAccountingContext, MemoryDomain,
    MemoryGrant, MemoryReleaseHandle, MemoryResult,
};
use paro_common::runtime_value::Value;
use paro_storage::buffer::MemoryTag;

fn grant_for_context(memory: &MemoryAccountingContext) -> MemoryGrant {
    if let Some(owner) = memory.owner() {
        MemoryGrant::new(0, memory.domain(), owner).expect("zero-byte aggregate grant should fit")
    } else {
        MemoryGrant::detached(usize::MAX / 4, memory.domain())
    }
}

fn value_row_memory_usage(values: &Vec<Value>) -> usize {
    values.capacity() * size_of::<Value>()
        + values.iter().map(Value::allocation_size).sum::<usize>()
}

/// A row of aggregate modifier values whose heap/storage bytes are released on drop.
#[derive(Debug)]
pub(super) struct AccountedValueRow {
    values: Vec<Value>,
    release: MemoryReleaseHandle,
}

impl AccountedValueRow {
    pub(super) fn new(memory: &MemoryAccountingContext, values: Vec<Value>) -> MemoryResult<Self> {
        let release = memory.retain(value_row_memory_usage(&values))?;
        Ok(Self { values, release })
    }

    pub(super) fn into_values(mut self) -> Vec<Value> {
        self.release.release();
        std::mem::take(&mut self.values)
    }
}

impl Deref for AccountedValueRow {
    type Target = [Value];

    fn deref(&self) -> &Self::Target {
        &self.values
    }
}

impl PartialEq for AccountedValueRow {
    fn eq(&self, other: &Self) -> bool {
        self.values == other.values
    }
}

impl Eq for AccountedValueRow {}

impl Hash for AccountedValueRow {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.values.hash(state);
    }
}

impl Drop for AccountedValueRow {
    fn drop(&mut self) {
        self.release.release();
    }
}

#[derive(Debug)]
pub(super) struct AccountedValueRows {
    memory: MemoryAccountingContext,
    rows: AccountedVec<AccountedValueRow>,
}

impl AccountedValueRows {
    pub(super) fn new(memory: MemoryAccountingContext) -> Self {
        let metadata_memory = memory.with_class(MemoryAccountingClass::Metadata);
        Self {
            memory,
            rows: AccountedVec::new_with_accounting(
                grant_for_context(&metadata_memory),
                MemoryTag::Metadata,
                MemoryAccountingClass::Metadata,
            ),
        }
    }

    pub(super) fn push(&mut self, values: Vec<Value>) -> MemoryResult<()> {
        let row = AccountedValueRow::new(&self.memory, values)?;
        self.rows.try_push(row)
    }

    pub(super) fn take(&mut self) -> Vec<AccountedValueRow> {
        self.rows.drain().collect()
    }
}

#[derive(Debug)]
pub(super) struct AccountedValueRowSet {
    memory: MemoryAccountingContext,
    rows: AccountedHashSet<AccountedValueRow>,
}

impl AccountedValueRowSet {
    pub(super) fn new(memory: MemoryAccountingContext) -> Self {
        let metadata_memory = memory.with_class(MemoryAccountingClass::Metadata);
        Self {
            memory,
            rows: AccountedHashSet::new_with_accounting(
                grant_for_context(&metadata_memory),
                MemoryTag::Metadata,
                MemoryAccountingClass::Metadata,
            ),
        }
    }

    pub(super) fn insert(&mut self, values: Vec<Value>) -> MemoryResult<bool> {
        let row = AccountedValueRow::new(&self.memory, values)?;
        self.rows.try_insert(row)
    }

    pub(super) fn into_rows(mut self) -> Vec<AccountedValueRow> {
        self.rows.drain().collect()
    }

    pub(super) fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }
}

pub(super) fn aggregate_modifier_memory_context(
    owner: Arc<dyn paro_common::memory::MemoryOwner>,
) -> MemoryAccountingContext {
    MemoryAccountingContext::from_owner(
        owner,
        MemoryDomain::Host,
        MemoryTag::HashTable,
        MemoryAccountingClass::Revocable,
    )
}
