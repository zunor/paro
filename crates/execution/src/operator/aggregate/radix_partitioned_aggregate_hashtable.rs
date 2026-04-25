// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Radix-partitioned grouped aggregate hash table.
//!
//! This wraps multiple [`GroupedAggregateHashTable`] partitions and routes
//! rows by hash high bits, so each partition resizes/scans independently.

use std::sync::Arc;

use paro_common::allocator::Allocator;
use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::types::LogicalType;
use paro_common::vector::{SelectionVector, Vector};

use super::aggregate_object::AggregateObject;
use super::grouped_aggregate_hashtable::{GroupedAggregateHashTable, HTScanPosition};

const MAX_RADIX_PARTITION_BITS: usize = 8;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RadixHTScanPosition {
    pub partition_idx: usize,
    pub partition_positions: Vec<HTScanPosition>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AggregateHTScanPosition {
    pub flat: HTScanPosition,
    pub radix: RadixHTScanPosition,
}

#[derive(Debug)]
pub enum AggregateHashTable {
    Flat(GroupedAggregateHashTable),
    Radix(RadixPartitionedAggregateHashTable),
}

impl AggregateHashTable {
    pub fn new_flat(
        group_types: Vec<LogicalType>,
        aggregate_objects: Vec<AggregateObject>,
        aggregate_inputs: Vec<Vec<usize>>,
        allocator: Arc<dyn Allocator>,
    ) -> Result<Self> {
        Ok(Self::Flat(GroupedAggregateHashTable::new(
            group_types,
            aggregate_objects,
            aggregate_inputs,
            allocator,
        )?))
    }

    pub fn new_radix(
        group_types: Vec<LogicalType>,
        aggregate_objects: Vec<AggregateObject>,
        aggregate_inputs: Vec<Vec<usize>>,
        partition_bits: usize,
        allocator: Arc<dyn Allocator>,
    ) -> Result<Self> {
        Ok(Self::Radix(RadixPartitionedAggregateHashTable::new(
            group_types,
            aggregate_objects,
            aggregate_inputs,
            partition_bits,
            allocator,
        )?))
    }

    pub fn hash_groups(&self, groups: &Chunk) -> Result<Vector> {
        match self {
            Self::Flat(table) => table.hash_groups(groups),
            Self::Radix(table) => table.hash_groups(groups),
        }
    }

    pub fn find_or_create_groups(
        &mut self,
        groups: &Chunk,
        hashes: &Vector,
        addresses: &mut Vector,
        new_groups: &mut SelectionVector,
    ) -> Result<usize> {
        match self {
            Self::Flat(table) => table.find_or_create_groups(groups, hashes, addresses, new_groups),
            Self::Radix(table) => {
                table.find_or_create_groups(groups, hashes, addresses, new_groups)
            }
        }
    }

    pub fn update_aggregates(
        &mut self,
        payload: &Chunk,
        hashes: Option<&Vector>,
        addresses: &Vector,
        filter: Option<&SelectionVector>,
    ) -> Result<()> {
        match self {
            Self::Flat(table) => table.update_aggregates(payload, addresses, filter),
            Self::Radix(table) => table.update_aggregates(payload, hashes, addresses, filter),
        }
    }

    pub fn combine(&mut self, other: &mut Self) -> Result<()> {
        match (self, other) {
            (Self::Flat(left), Self::Flat(right)) => left.combine(right),
            (Self::Radix(left), Self::Radix(right)) => left.combine(right),
            (left, right) => Err(paro_error::internal(format!(
                "Cannot combine aggregate hash tables with different implementations: left={:?}, right={:?}",
                left.table_kind(),
                right.table_kind()
            ))),
        }
    }

    pub fn scan(
        &mut self,
        position: &mut AggregateHTScanPosition,
        result: &mut Chunk,
    ) -> Result<bool> {
        match self {
            Self::Flat(table) => table.scan(&mut position.flat, result),
            Self::Radix(table) => table.scan(&mut position.radix, result),
        }
    }

    pub fn destroy(&mut self) -> Result<()> {
        match self {
            Self::Flat(table) => table.destroy(),
            Self::Radix(table) => table.destroy(),
        }
    }

    pub fn inline_key_width(&self) -> Option<usize> {
        match self {
            Self::Flat(table) => table.inline_key_width(),
            Self::Radix(table) => table.inline_key_width(),
        }
    }

    pub fn radix_partition_count(&self) -> Option<usize> {
        match self {
            Self::Flat(_) => None,
            Self::Radix(table) => Some(table.partition_count()),
        }
    }

    pub fn memory_usage(&self) -> usize {
        match self {
            Self::Flat(table) => table.memory_usage(),
            Self::Radix(table) => table.memory_usage(),
        }
    }

    pub fn external_accounted_memory_usage(&self) -> usize {
        match self {
            Self::Flat(table) => table.external_accounted_memory_usage(),
            Self::Radix(table) => table.external_accounted_memory_usage(),
        }
    }

    pub fn count(&self) -> usize {
        match self {
            Self::Flat(table) => table.count(),
            Self::Radix(table) => table.count(),
        }
    }

    pub fn allocator(&self) -> Arc<dyn Allocator> {
        match self {
            Self::Flat(table) => table.allocator(),
            Self::Radix(table) => table.allocator(),
        }
    }

    fn table_kind(&self) -> &'static str {
        match self {
            Self::Flat(_) => "flat",
            Self::Radix(_) => "radix",
        }
    }
}

#[derive(Debug)]
pub struct RadixPartitionedAggregateHashTable {
    group_types: Vec<LogicalType>,
    partition_bits: usize,
    partition_mask: usize,
    partitions: Vec<GroupedAggregateHashTable>,
}

impl RadixPartitionedAggregateHashTable {
    pub fn new(
        group_types: Vec<LogicalType>,
        aggregate_objects: Vec<AggregateObject>,
        aggregate_inputs: Vec<Vec<usize>>,
        partition_bits: usize,
        allocator: Arc<dyn Allocator>,
    ) -> Result<Self> {
        if partition_bits == 0 || partition_bits > MAX_RADIX_PARTITION_BITS {
            return Err(paro_error::internal(format!(
                "Invalid radix partition bits for aggregate hash table: bits={partition_bits}, allowed=1..={MAX_RADIX_PARTITION_BITS}"
            )));
        }
        let partition_count = 1usize.checked_shl(partition_bits as u32).ok_or_else(|| {
            paro_error::internal(format!(
                "Radix partition count overflow for bits={partition_bits}"
            ))
        })?;
        let mut partitions = Vec::with_capacity(partition_count);
        for _ in 0..partition_count {
            partitions.push(GroupedAggregateHashTable::new(
                group_types.clone(),
                aggregate_objects.clone(),
                aggregate_inputs.clone(),
                allocator.clone(),
            )?);
        }
        Ok(Self {
            group_types,
            partition_bits,
            partition_mask: partition_count - 1,
            partitions,
        })
    }

    pub fn partition_count(&self) -> usize {
        self.partitions.len()
    }

    pub fn inline_key_width(&self) -> Option<usize> {
        self.partitions
            .first()
            .and_then(GroupedAggregateHashTable::inline_key_width)
    }

    pub fn hash_groups(&self, groups: &Chunk) -> Result<Vector> {
        self.partitions
            .first()
            .ok_or_else(|| {
                paro_error::internal("Radix aggregate hash table has no partitions".to_string())
            })?
            .hash_groups(groups)
    }

    pub fn find_or_create_groups(
        &mut self,
        groups: &Chunk,
        hashes: &Vector,
        addresses: &mut Vector,
        new_groups: &mut SelectionVector,
    ) -> Result<usize> {
        validate_hashes(hashes, groups.size())?;
        validate_address_capacity(addresses, groups.size())?;

        if groups.size() == 0 {
            addresses.try_set_count(0)?;
            *new_groups =
                SelectionVector::try_from_indices(Vec::new(), groups.allocator().clone())?;
            return Ok(0);
        }

        let mut partition_rows = vec![Vec::<usize>::new(); self.partitions.len()];
        let mut partition_hashes = vec![Vec::<u64>::new(); self.partitions.len()];

        let hash_format = hashes.try_decode_ref(groups.size())?;
        let hash_data = hash_format.get_data::<u64>();
        for row_idx in 0..groups.size() {
            let physical_idx = hash_format.physical_index(row_idx);
            if !hash_format.validity().is_valid(physical_idx) {
                return Err(paro_error::internal(format!(
                    "Group hash contains NULL at row {row_idx}"
                )));
            }
            let hash = unsafe { *hash_data.add(physical_idx) };
            let partition_idx = self.partition_for_hash(hash);
            partition_rows[partition_idx].push(row_idx);
            partition_hashes[partition_idx].push(hash);
        }

        addresses.try_set_count(groups.size())?;
        let address_data = unsafe { addresses.flat_data_mut::<*mut u8>() };
        let mut new_group_rows = Vec::new();

        for partition_idx in 0..self.partitions.len() {
            let rows = partition_rows.get(partition_idx).ok_or_else(|| {
                paro_error::internal(format!(
                    "Radix partition index out of bounds during find_or_create: partition_idx={partition_idx}"
                ))
            })?;
            if rows.is_empty() {
                continue;
            }

            let partition = self.partitions.get_mut(partition_idx).ok_or_else(|| {
                paro_error::internal(format!(
                    "Radix partition index out of bounds: partition_idx={partition_idx}"
                ))
            })?;
            let partition_groups = gather_chunk_rows(groups, rows)?;
            let partition_hashes_vec = u64_vector_from_slice(
                partition_hashes.get(partition_idx).ok_or_else(|| {
                    paro_error::internal(format!(
                        "Missing partition hashes while probing radix table: partition_idx={partition_idx}"
                    ))
                })?,
                groups.allocator().clone(),
            )?;
            let mut partition_addresses =
                Vector::try_new(LogicalType::BigInt, rows.len(), groups.allocator().clone())?;
            let mut partition_new_groups =
                SelectionVector::try_with_capacity(rows.len(), groups.allocator().clone())?;
            partition.find_or_create_groups(
                &partition_groups,
                &partition_hashes_vec,
                &mut partition_addresses,
                &mut partition_new_groups,
            )?;

            let partition_address_format = partition_addresses.try_decode_ref(rows.len())?;
            let partition_address_data = partition_address_format.get_data::<*mut u8>();
            for (local_row, &global_row) in rows.iter().enumerate() {
                let physical_idx = partition_address_format.physical_index(local_row);
                if !partition_address_format.validity().is_valid(physical_idx) {
                    return Err(paro_error::internal(format!(
                        "Partition address vector contains NULL: partition_idx={partition_idx}, local_row={local_row}"
                    )));
                }
                let state_ptr = unsafe { *partition_address_data.add(physical_idx) };
                unsafe {
                    *address_data.add(global_row) = state_ptr;
                }
            }

            for idx in 0..partition_new_groups.len() {
                let local_row = partition_new_groups.get(idx);
                if local_row >= rows.len() {
                    return Err(paro_error::internal(format!(
                        "Partition new-group index out of bounds: partition_idx={partition_idx}, local_row={local_row}, partition_rows={}",
                        rows.len()
                    )));
                }
                new_group_rows.push(rows[local_row] as u32);
            }
        }

        *new_groups =
            SelectionVector::try_from_indices(new_group_rows, groups.allocator().clone())?;
        Ok(new_groups.len())
    }

    pub fn update_aggregates(
        &mut self,
        payload: &Chunk,
        hashes: Option<&Vector>,
        addresses: &Vector,
        filter: Option<&SelectionVector>,
    ) -> Result<()> {
        if payload.size() == 0 {
            return Ok(());
        }
        if filter.is_some() {
            return Err(paro_error::internal(
                "Radix partitioned aggregate hash table does not support filtered updates directly"
                    .to_string(),
            ));
        }
        let hashes = hashes.ok_or_else(|| {
            paro_error::internal(
                "Radix partitioned aggregate hash table requires hash vector for updates"
                    .to_string(),
            )
        })?;
        validate_hashes(hashes, payload.size())?;
        if addresses.len() < payload.size() {
            return Err(paro_error::internal(format!(
                "Address vector too small for radix aggregate update: addresses={} payload_rows={}",
                addresses.len(),
                payload.size()
            )));
        }

        let mut partition_rows = vec![Vec::<usize>::new(); self.partitions.len()];
        let hash_format = hashes.try_decode_ref(payload.size())?;
        let hash_data = hash_format.get_data::<u64>();
        for row_idx in 0..payload.size() {
            let physical_idx = hash_format.physical_index(row_idx);
            if !hash_format.validity().is_valid(physical_idx) {
                return Err(paro_error::internal(format!(
                    "Group hash contains NULL at row {row_idx}"
                )));
            }
            let hash = unsafe { *hash_data.add(physical_idx) };
            let partition_idx = self.partition_for_hash(hash);
            partition_rows[partition_idx].push(row_idx);
        }

        for partition_idx in 0..self.partitions.len() {
            let rows = partition_rows.get(partition_idx).ok_or_else(|| {
                paro_error::internal(format!(
                    "Radix partition index out of bounds during update: partition_idx={partition_idx}"
                ))
            })?;
            if rows.is_empty() {
                continue;
            }
            let partition = self.partitions.get_mut(partition_idx).ok_or_else(|| {
                paro_error::internal(format!(
                    "Radix partition index out of bounds during update: partition_idx={partition_idx}"
                ))
            })?;
            let partition_payload = gather_chunk_rows(payload, rows)?;
            let partition_addresses = gather_address_rows(addresses, rows)?;
            partition.update_aggregates(&partition_payload, &partition_addresses, None)?;
        }

        Ok(())
    }

    pub fn combine(&mut self, other: &mut Self) -> Result<()> {
        if self.partition_bits != other.partition_bits
            || self.group_types != other.group_types
            || self.partitions.len() != other.partitions.len()
        {
            return Err(paro_error::internal(format!(
                "Cannot combine radix aggregate hash tables with different layouts: \
bits {}/{} partitions {}/{} group_types {:?}/{:?}",
                self.partition_bits,
                other.partition_bits,
                self.partitions.len(),
                other.partitions.len(),
                self.group_types,
                other.group_types
            )));
        }
        for partition_idx in 0..self.partitions.len() {
            let left = self.partitions.get_mut(partition_idx).ok_or_else(|| {
                paro_error::internal(format!(
                    "Radix partition index out of bounds during combine: partition_idx={partition_idx}"
                ))
            })?;
            let right = other.partitions.get_mut(partition_idx).ok_or_else(|| {
                paro_error::internal(format!(
                    "Radix partition index out of bounds during combine: partition_idx={partition_idx}"
                ))
            })?;
            left.combine(right)?;
        }
        Ok(())
    }

    pub fn scan(&mut self, position: &mut RadixHTScanPosition, result: &mut Chunk) -> Result<bool> {
        if position.partition_positions.len() != self.partitions.len() {
            position.partition_positions = vec![HTScanPosition::default(); self.partitions.len()];
            position.partition_idx = 0;
        }
        while position.partition_idx < self.partitions.len() {
            let partition_idx = position.partition_idx;
            let partition = self.partitions.get_mut(partition_idx).ok_or_else(|| {
                paro_error::internal(format!(
                    "Radix partition index out of bounds during scan: partition_idx={partition_idx}"
                ))
            })?;
            let part_position = position
                .partition_positions
                .get_mut(partition_idx)
                .ok_or_else(|| {
                    paro_error::internal(format!(
                        "Radix partition scan position missing: partition_idx={partition_idx}"
                    ))
                })?;
            if partition.scan(part_position, result)? {
                return Ok(true);
            }
            position.partition_idx += 1;
        }
        result.try_set_cardinality(0)?;
        Ok(false)
    }

    pub fn destroy(&mut self) -> Result<()> {
        for partition in &mut self.partitions {
            partition.destroy()?;
        }
        Ok(())
    }

    pub fn memory_usage(&self) -> usize {
        self.partitions
            .iter()
            .map(GroupedAggregateHashTable::memory_usage)
            .sum()
    }

    pub fn external_accounted_memory_usage(&self) -> usize {
        self.partitions
            .iter()
            .map(GroupedAggregateHashTable::external_accounted_memory_usage)
            .sum()
    }

    pub fn count(&self) -> usize {
        self.partitions
            .iter()
            .map(GroupedAggregateHashTable::count)
            .sum()
    }

    pub fn allocator(&self) -> Arc<dyn Allocator> {
        self.partitions
            .first()
            .map(GroupedAggregateHashTable::allocator)
            .expect("radix aggregate hash table should have partitions")
    }

    fn partition_for_hash(&self, hash: u64) -> usize {
        let shift = (u64::BITS as usize).saturating_sub(self.partition_bits);
        ((hash >> shift) as usize) & self.partition_mask
    }
}

fn validate_hashes(hashes: &Vector, row_count: usize) -> Result<()> {
    if hashes.logical_type() != &LogicalType::UBigInt {
        return Err(paro_error::internal(format!(
            "Hash vector type must be UBigInt, found {:?}",
            hashes.logical_type()
        )));
    }
    if hashes.len() < row_count {
        return Err(paro_error::internal(format!(
            "Hash vector too small: required={row_count}, actual={}",
            hashes.len()
        )));
    }
    Ok(())
}

fn validate_address_capacity(addresses: &Vector, row_count: usize) -> Result<()> {
    if addresses.capacity() < row_count {
        return Err(paro_error::internal(format!(
            "Address vector capacity too small: required={row_count}, capacity={}",
            addresses.capacity()
        )));
    }
    Ok(())
}

fn u64_vector_from_slice(values: &[u64], allocator: Arc<dyn Allocator>) -> Result<Vector> {
    let mut vector = Vector::try_new(LogicalType::UBigInt, values.len(), allocator)?;
    vector.try_set_count(values.len())?;
    unsafe {
        let data = vector.flat_data_mut::<u64>();
        for (idx, value) in values.iter().enumerate() {
            *data.add(idx) = *value;
        }
    }
    Ok(vector)
}

fn gather_chunk_rows(source: &Chunk, rows: &[usize]) -> Result<Chunk> {
    let mut column_types = Vec::with_capacity(source.column_count());
    for column_idx in 0..source.column_count() {
        let column = source.column(column_idx).ok_or_else(|| {
            paro_error::internal(format!(
                "Missing source column while gathering chunk rows: column_idx={column_idx}"
            ))
        })?;
        column_types.push(column.logical_type().clone());
    }
    let mut gathered =
        Chunk::try_initialize(&column_types, rows.len(), source.allocator().clone())?;
    gathered.try_set_cardinality(rows.len())?;
    for column_idx in 0..source.column_count() {
        let source_col = source.column(column_idx).ok_or_else(|| {
            paro_error::internal(format!(
                "Missing source column while copying gathered chunk rows: column_idx={column_idx}"
            ))
        })?;
        let target_col = gathered.column_mut(column_idx).ok_or_else(|| {
            paro_error::internal(format!(
                "Missing target column while gathering chunk rows: column_idx={column_idx}"
            ))
        })?;
        for (target_row, source_row) in rows.iter().copied().enumerate() {
            if source_row >= source.size() {
                return Err(paro_error::internal(format!(
                    "Gather row index out of bounds: source_row={source_row}, source_size={}",
                    source.size()
                )));
            }
            target_col.try_copy_at(target_row, source_col.as_ref(), source_row)?;
        }
    }
    Ok(gathered)
}

fn gather_address_rows(addresses: &Vector, rows: &[usize]) -> Result<Vector> {
    let mut gathered = Vector::try_new(
        LogicalType::BigInt,
        rows.len(),
        addresses.allocator().clone(),
    )?;
    gathered.try_set_count(rows.len())?;

    let address_format = addresses.try_decode_ref(addresses.len())?;
    let address_data = address_format.get_data::<*mut u8>();
    unsafe {
        let target_data = gathered.flat_data_mut::<*mut u8>();
        for (target_idx, source_row) in rows.iter().copied().enumerate() {
            if source_row >= addresses.len() {
                return Err(paro_error::internal(format!(
                    "Address gather row index out of bounds: source_row={source_row}, addresses={}",
                    addresses.len()
                )));
            }
            let physical_idx = address_format.physical_index(source_row);
            if !address_format.validity().is_valid(physical_idx) {
                return Err(paro_error::internal(format!(
                    "Address vector contains NULL while gathering rows: source_row={source_row}"
                )));
            }
            *target_data.add(target_idx) = *address_data.add(physical_idx);
        }
    }

    Ok(gathered)
}
