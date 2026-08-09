// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Build-side domains used to create hash-join runtime filters.
//!
//! Exact fixed-width domains stay in their physical representation while a
//! join is built. They are merged by ownership and frozen into sorted arrays
//! before publication. Generic values retain only min/max statistics. This
//! keeps boxed [`Value`] conversion at the storage-predicate boundary rather
//! than on every build row.

use paro_common::allocator::MemoryTag;
use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::memory::{
    AccountedVec, MemoryAccountingClass, MemoryAccountingContext, MemoryReleaseHandle,
};
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use paro_common::vector::{DataRef, SelectionVector, Vector, VECTOR_SIZE};
use paro_storage::index::{
    ColumnId, FixedMembership, FixedMembershipBuildPolicy, Predicate, PredicateTree,
};

/// Bounded construction policy for analytical join filters.
///
/// The mutable domain is capped independently from its frozen representation.
/// A frozen dense set can spend up to 8 MiB and at most 256 bits per retained
/// value, which covers fact-table key domains without allowing sparse endpoint
/// ranges to dictate allocation size. Domains beyond the exact-value budget
/// retain min/max only.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct JoinRuntimeFilterPolicy {
    max_exact_values: usize,
    membership: FixedMembershipBuildPolicy,
}

const MIN_EXACT_PENDING_VALUES: usize = VECTOR_SIZE;

impl Default for JoinRuntimeFilterPolicy {
    fn default() -> Self {
        Self {
            max_exact_values: 512 * 1024,
            membership: FixedMembershipBuildPolicy::new(64 * 1024 * 1024, 256),
        }
    }
}

#[derive(Debug)]
struct RuntimeFilterReservation(MemoryReleaseHandle);

impl Drop for RuntimeFilterReservation {
    fn drop(&mut self) {
        self.0.release();
    }
}

#[derive(Debug)]
enum ExactValues<T> {
    Enabled {
        values: AccountedVec<T>,
        /// Length of the sorted, unique prefix. Values after this boundary are
        /// an unsorted append buffer.
        canonical_len: usize,
        memory: MemoryAccountingContext,
    },
    Disabled,
}

#[derive(Debug)]
struct FrozenExactValues {
    values: Option<FixedMembership>,
    _reservation: Option<RuntimeFilterReservation>,
}

impl<T> ExactValues<T>
where
    T: Copy + Eq + Ord,
{
    fn mutable(memory: MemoryAccountingContext) -> Self {
        let Ok(grant) = memory.grant() else {
            return Self::Disabled;
        };
        Self::Enabled {
            values: AccountedVec::new_with_accounting(
                grant,
                memory.tag(),
                memory.accounting_class(),
            ),
            canonical_len: 0,
            memory,
        }
    }

    fn insert(&mut self, value: T, max_values: usize) {
        let Self::Enabled {
            values,
            canonical_len,
            ..
        } = self
        else {
            return;
        };
        if max_values == 0 {
            *self = Self::Disabled;
            return;
        }

        // A full canonical domain can decide immediately without growing the
        // pending buffer. Below the budget, values append linearly and are
        // normalized only after the suffix has accumulated enough work to pay
        // for sorting the existing prefix.
        if *canonical_len == values.len() && values.len() == max_values {
            if values.binary_search(&value).is_err() {
                *self = Self::Disabled;
            }
            return;
        }
        if values.try_push(value).is_err() {
            // Exact membership is optional. Query-memory pressure degrades to
            // the min/max domain maintained alongside this set.
            *self = Self::Disabled;
            return;
        }
        let pending_len = values.len().saturating_sub(*canonical_len);
        if pending_len >= exact_pending_limit(*canonical_len, max_values) {
            normalize_exact_values(values, canonical_len);
            if values.len() > max_values {
                *self = Self::Disabled;
            }
        }
    }

    /// Geometrically reserve the portion of an input batch that fits before
    /// the next normalization boundary.
    ///
    /// `AccountedVec` deliberately uses exact reservations so its query-memory
    /// charge matches the physical allocation. Growing it one value at a time
    /// would consequently put allocator and accounting work in the build-row
    /// loop. The sorted-prefix/pending-suffix representation caps low-NDV
    /// domains near one vector while allowing high-NDV domains to double up to
    /// the 2M worst-case bound. A failed reservation only disables optional
    /// exact membership; min/max collection continues independently.
    fn prepare_batch(&mut self, additional: usize, max_values: usize) {
        let Self::Enabled {
            values,
            canonical_len,
            ..
        } = self
        else {
            return;
        };
        if max_values == 0
            || (*canonical_len == values.len() && values.len() == max_values)
            || additional == 0
        {
            return;
        }
        let buffer_limit =
            canonical_len.saturating_add(exact_pending_limit(*canonical_len, max_values));
        let required = values.len().saturating_add(additional).min(buffer_limit);
        if required <= values.capacity() {
            return;
        }
        let geometric = values.capacity().max(1).saturating_mul(2);
        let target_capacity = required.max(geometric).min(buffer_limit);
        if values
            .try_reserve(target_capacity.saturating_sub(values.len()))
            .is_err()
        {
            *self = Self::Disabled;
        }
    }

    fn merge(&mut self, incoming: Self, max_values: usize) {
        match incoming {
            Self::Disabled => *self = Self::Disabled,
            Self::Enabled {
                values: mut incoming,
                canonical_len: mut incoming_canonical_len,
                memory: incoming_memory,
            } => {
                normalize_exact_values(&mut incoming, &mut incoming_canonical_len);
                if incoming.len() > max_values {
                    *self = Self::Disabled;
                    return;
                }
                let Self::Enabled {
                    values,
                    canonical_len,
                    memory,
                } = self
                else {
                    return;
                };
                normalize_exact_values(values, canonical_len);
                if values.len() > max_values {
                    *self = Self::Disabled;
                    return;
                }
                if values.is_empty() {
                    // The global builder starts empty. Adopt the first local
                    // batch together with its accounting context without a
                    // copy or a second allocation.
                    std::mem::swap(values, &mut incoming);
                    *canonical_len = incoming_canonical_len;
                    *memory = incoming_memory;
                    return;
                }
                if incoming.is_empty() {
                    return;
                }
                let normalized_len = values.len();
                let missing = incoming
                    .iter()
                    .filter(|value| values.binary_search(value).is_err())
                    .count();
                if missing > max_values.saturating_sub(normalized_len) {
                    *self = Self::Disabled;
                    return;
                }
                if missing > 0 && values.try_reserve(missing).is_err() {
                    *self = Self::Disabled;
                    return;
                }
                for value in incoming.iter().copied() {
                    if values[..normalized_len].binary_search(&value).is_ok() {
                        continue;
                    }
                    // `incoming` is normalized, so appended values cannot
                    // duplicate one another. Only the original sorted prefix
                    // needs a lookup while the suffix is appended.
                    if values.try_push(value).is_err() {
                        *self = Self::Disabled;
                        return;
                    }
                }
                if values.len() != normalized_len {
                    normalize_exact_values(values, canonical_len);
                }
            }
        }
    }

    fn freeze_with(
        mut self,
        max_values: usize,
        freeze: impl FnOnce(Vec<T>) -> FixedMembership,
    ) -> FrozenExactValues {
        let Self::Enabled {
            values,
            canonical_len,
            memory,
        } = &mut self
        else {
            return FrozenExactValues {
                values: None,
                _reservation: None,
            };
        };
        normalize_exact_values(values, canonical_len);
        if values.len() > max_values {
            return FrozenExactValues {
                values: None,
                _reservation: None,
            };
        }
        let frozen = freeze(values.drain().collect());
        if frozen.is_contiguous() {
            return FrozenExactValues {
                values: None,
                _reservation: None,
            };
        }
        let Ok(reservation) = memory.retain(frozen.allocation_size()) else {
            return FrozenExactValues {
                values: None,
                _reservation: None,
            };
        };
        FrozenExactValues {
            values: Some(frozen),
            _reservation: Some(RuntimeFilterReservation(reservation)),
        }
    }
}

fn exact_pending_limit(canonical_len: usize, max_values: usize) -> usize {
    canonical_len
        .max(MIN_EXACT_PENDING_VALUES)
        .min(max_values.max(1))
}

fn normalize_exact_values<T: Copy + Eq + Ord>(
    values: &mut AccountedVec<T>,
    canonical_len: &mut usize,
) {
    if *canonical_len == values.len() {
        return;
    }
    if values.len() < 2 {
        *canonical_len = values.len();
        return;
    }
    values.sort_unstable();
    let mut write = 1usize;
    for read in 1..values.len() {
        if values[read] != values[write - 1] {
            values[write] = values[read];
            write += 1;
        }
    }
    values.truncate(write);
    *canonical_len = write;
}

#[derive(Debug)]
struct ExactDomainBuilder<T> {
    min: Option<T>,
    max: Option<T>,
    values: ExactValues<T>,
    policy: JoinRuntimeFilterPolicy,
}

impl<T> ExactDomainBuilder<T>
where
    T: Copy + Eq + Ord,
{
    fn new(policy: JoinRuntimeFilterPolicy, memory: MemoryAccountingContext) -> Self {
        Self {
            min: None,
            max: None,
            values: ExactValues::mutable(memory),
            policy,
        }
    }

    #[inline]
    fn add(&mut self, value: T) {
        self.min = Some(self.min.map_or(value, |current| current.min(value)));
        self.max = Some(self.max.map_or(value, |current| current.max(value)));
        self.values.insert(value, self.policy.max_exact_values);
    }

    fn prepare_batch(&mut self, additional: usize) {
        self.values
            .prepare_batch(additional, self.policy.max_exact_values);
    }

    fn merge(&mut self, incoming: Self) {
        if let Some(value) = incoming.min {
            self.min = Some(self.min.map_or(value, |current| current.min(value)));
        }
        if let Some(value) = incoming.max {
            self.max = Some(self.max.map_or(value, |current| current.max(value)));
        }
        if self.policy != incoming.policy {
            debug_assert_eq!(
                self.policy, incoming.policy,
                "runtime filter policies must match across local sketches"
            );
            self.values = ExactValues::Disabled;
            return;
        }
        self.values
            .merge(incoming.values, self.policy.max_exact_values);
    }

    fn freeze_with(
        self,
        freeze: impl FnOnce(Vec<T>, FixedMembershipBuildPolicy) -> FixedMembership,
    ) -> FrozenExactDomain<T> {
        let membership = self.policy.membership;
        FrozenExactDomain {
            min: self.min,
            max: self.max,
            values: self
                .values
                .freeze_with(self.policy.max_exact_values, |values| {
                    freeze(values, membership)
                }),
        }
    }
}

#[derive(Debug)]
struct FrozenExactDomain<T> {
    min: Option<T>,
    max: Option<T>,
    values: FrozenExactValues,
}

#[derive(Debug)]
struct GenericDomain {
    comparable: bool,
    min: Option<Value>,
    max: Option<Value>,
}

impl GenericDomain {
    fn new() -> Self {
        Self {
            comparable: true,
            min: None,
            max: None,
        }
    }

    fn add_value(&mut self, value: Value) {
        Self::update_extreme(&mut self.min, &value, &mut self.comparable, true);
        if self.comparable {
            Self::update_extreme(&mut self.max, &value, &mut self.comparable, false);
        }
    }

    fn add_string(&mut self, value: &str) {
        Self::update_string_extreme(&mut self.min, value, &mut self.comparable, true);
        if self.comparable {
            Self::update_string_extreme(&mut self.max, value, &mut self.comparable, false);
        }
    }

    fn merge(&mut self, incoming: Self) {
        if !incoming.comparable {
            self.comparable = false;
        }
        if !self.comparable {
            return;
        }
        if let Some(value) = incoming.min {
            Self::update_extreme(&mut self.min, &value, &mut self.comparable, true);
        }
        if let Some(value) = incoming.max {
            Self::update_extreme(&mut self.max, &value, &mut self.comparable, false);
        }
    }

    fn update_extreme(
        slot: &mut Option<Value>,
        value: &Value,
        comparable: &mut bool,
        is_min: bool,
    ) {
        let Some(current) = slot.as_ref() else {
            *slot = Some(value.clone());
            return;
        };
        let Some(ordering) = value.partial_cmp(current) else {
            *comparable = false;
            return;
        };
        let replace = if is_min {
            ordering == std::cmp::Ordering::Less
        } else {
            ordering == std::cmp::Ordering::Greater
        };
        if replace {
            *slot = Some(value.clone());
        }
    }

    fn update_string_extreme(
        slot: &mut Option<Value>,
        value: &str,
        comparable: &mut bool,
        is_min: bool,
    ) {
        let Some(current) = slot.as_ref() else {
            *slot = Some(Value::Varchar(value.to_owned()));
            return;
        };
        let Value::Varchar(current) = current else {
            *comparable = false;
            return;
        };
        let replace = if is_min {
            value < current.as_str()
        } else {
            value > current.as_str()
        };
        if replace {
            *slot = Some(Value::Varchar(value.to_owned()));
        }
    }
}

#[derive(Debug)]
enum RuntimeFilterDomainBuilder {
    I32(ExactDomainBuilder<i32>),
    I64(ExactDomainBuilder<i64>),
    I128(ExactDomainBuilder<i128>),
    Generic(GenericDomain),
}

impl RuntimeFilterDomainBuilder {
    fn for_type(
        logical_type: &LogicalType,
        policy: JoinRuntimeFilterPolicy,
        memory: MemoryAccountingContext,
    ) -> Self {
        match logical_type {
            LogicalType::Integer | LogicalType::Date => {
                Self::I32(ExactDomainBuilder::new(policy, memory))
            }
            LogicalType::BigInt
            | LogicalType::Decimal {
                precision: 0..=18, ..
            } => Self::I64(ExactDomainBuilder::new(policy, memory)),
            LogicalType::Decimal { .. } => Self::I128(ExactDomainBuilder::new(policy, memory)),
            _ => Self::Generic(GenericDomain::new()),
        }
    }

    fn freeze(self) -> RuntimeFilterDomain {
        match self {
            Self::I32(domain) => {
                RuntimeFilterDomain::I32(domain.freeze_with(FixedMembership::i32_with_policy))
            }
            Self::I64(domain) => {
                RuntimeFilterDomain::I64(domain.freeze_with(FixedMembership::i64_with_policy))
            }
            Self::I128(domain) => {
                RuntimeFilterDomain::I128(domain.freeze_with(FixedMembership::i128_with_policy))
            }
            Self::Generic(domain) => RuntimeFilterDomain::Generic(domain),
        }
    }
}

#[derive(Debug)]
enum RuntimeFilterDomain {
    I32(FrozenExactDomain<i32>),
    I64(FrozenExactDomain<i64>),
    I128(FrozenExactDomain<i128>),
    Generic(GenericDomain),
}

#[derive(Debug)]
pub struct JoinRuntimeFilterBuilder {
    keys: Box<[JoinRuntimeFilterKeyBuilder]>,
}

impl JoinRuntimeFilterBuilder {
    pub fn empty(key_types: &[LogicalType]) -> Self {
        Self::empty_with_policy(
            key_types,
            JoinRuntimeFilterPolicy::default(),
            MemoryAccountingContext::detached(
                MemoryTag::HashTable,
                MemoryAccountingClass::Metadata,
            ),
        )
    }

    pub(crate) fn empty_with_memory(
        key_types: &[LogicalType],
        memory: MemoryAccountingContext,
    ) -> Self {
        Self::empty_with_policy(key_types, JoinRuntimeFilterPolicy::default(), memory)
    }

    fn empty_with_policy(
        key_types: &[LogicalType],
        policy: JoinRuntimeFilterPolicy,
        memory: MemoryAccountingContext,
    ) -> Self {
        Self {
            keys: key_types
                .iter()
                .cloned()
                .map(|logical_type| {
                    JoinRuntimeFilterKeyBuilder::new(logical_type, policy, memory.clone())
                })
                .collect::<Vec<_>>()
                .into_boxed_slice(),
        }
    }

    #[cfg(test)]
    pub(crate) fn empty_with_exact_value_limit(
        key_types: &[LogicalType],
        max_exact_values: usize,
    ) -> Self {
        Self::empty_with_policy(
            key_types,
            JoinRuntimeFilterPolicy {
                max_exact_values,
                ..JoinRuntimeFilterPolicy::default()
            },
            MemoryAccountingContext::detached(
                MemoryTag::HashTable,
                MemoryAccountingClass::Metadata,
            ),
        )
    }

    pub fn add_key_chunk(
        &mut self,
        keys: &Chunk,
        selection: &SelectionVector,
        selected_count: usize,
    ) -> Result<()> {
        if keys.column_count() != self.keys.len() {
            return Err(paro_error::internal(format!(
                "hash join runtime filter key count mismatch: sketch={}, chunk={}",
                self.keys.len(),
                keys.column_count()
            )));
        }
        if selected_count > selection.len() {
            return Err(paro_error::internal(format!(
                "hash join runtime filter selected count exceeds selection length: selected={selected_count}, selection={}",
                selection.len()
            )));
        }
        for (key_idx, key) in self.keys.iter_mut().enumerate() {
            let vector = keys.column(key_idx).ok_or_else(|| {
                paro_error::internal("hash join runtime filter key column missing")
            })?;
            key.add_selected(vector, keys.size(), selection, selected_count)?;
        }
        Ok(())
    }

    pub(crate) fn merge(&mut self, incoming: Self) -> Result<()> {
        if self.keys.len() != incoming.keys.len() {
            return Err(paro_error::internal(format!(
                "hash join runtime filter sketch merge key count mismatch: left={}, right={}",
                self.keys.len(),
                incoming.keys.len()
            )));
        }
        for (left, right) in self.keys.iter_mut().zip(incoming.keys.into_vec()) {
            left.merge(right)?;
        }
        Ok(())
    }

    pub(crate) fn freeze(self) -> JoinRuntimeFilter {
        JoinRuntimeFilter {
            keys: self
                .keys
                .into_vec()
                .into_iter()
                .map(JoinRuntimeFilterKeyBuilder::freeze)
                .collect::<Vec<_>>()
                .into_boxed_slice(),
        }
    }
}

#[derive(Debug)]
struct JoinRuntimeFilterKeyBuilder {
    logical_type: LogicalType,
    non_null_count: u64,
    domain: RuntimeFilterDomainBuilder,
}

impl JoinRuntimeFilterKeyBuilder {
    fn new(
        logical_type: LogicalType,
        policy: JoinRuntimeFilterPolicy,
        memory: MemoryAccountingContext,
    ) -> Self {
        Self {
            domain: RuntimeFilterDomainBuilder::for_type(&logical_type, policy, memory),
            logical_type,
            non_null_count: 0,
        }
    }

    fn add_selected(
        &mut self,
        vector: &Vector,
        logical_count: usize,
        selection: &SelectionVector,
        selected_count: usize,
    ) -> Result<()> {
        if vector.logical_type() != &self.logical_type {
            return Err(paro_error::internal(format!(
                "hash join runtime filter key type mismatch: sketch={}, vector={}",
                self.logical_type,
                vector.logical_type()
            )));
        }

        let selected = &selection.as_slice()[..selected_count];
        match &mut self.domain {
            RuntimeFilterDomainBuilder::I32(domain) => {
                domain.prepare_batch(selected_count);
                visit_fixed(vector, logical_count, selected, Vector::get_i32, |value| {
                    add_exact_value(domain, &mut self.non_null_count, value)
                })
            }
            RuntimeFilterDomainBuilder::I64(domain) => {
                domain.prepare_batch(selected_count);
                visit_fixed(vector, logical_count, selected, Vector::get_i64, |value| {
                    add_exact_value(domain, &mut self.non_null_count, value)
                })
            }
            RuntimeFilterDomainBuilder::I128(domain) => {
                domain.prepare_batch(selected_count);
                visit_fixed(vector, logical_count, selected, Vector::get_i128, |value| {
                    add_exact_value(domain, &mut self.non_null_count, value)
                })
            }
            RuntimeFilterDomainBuilder::Generic(domain) => {
                for &row in selected {
                    let row_idx = row as usize;
                    if vector.is_null(row_idx) {
                        continue;
                    }
                    self.non_null_count += 1;
                    add_generic_vector_value(domain, &self.logical_type, vector, row_idx)?;
                }
                Ok(())
            }
        }
    }

    fn merge(&mut self, incoming: Self) -> Result<()> {
        if self.logical_type != incoming.logical_type {
            return Err(paro_error::internal(format!(
                "hash join runtime filter key sketch merge type mismatch: left={}, right={}",
                self.logical_type, incoming.logical_type
            )));
        }
        self.non_null_count += incoming.non_null_count;
        match (&mut self.domain, incoming.domain) {
            (RuntimeFilterDomainBuilder::I32(left), RuntimeFilterDomainBuilder::I32(right)) => {
                left.merge(right)
            }
            (RuntimeFilterDomainBuilder::I64(left), RuntimeFilterDomainBuilder::I64(right)) => {
                left.merge(right)
            }
            (RuntimeFilterDomainBuilder::I128(left), RuntimeFilterDomainBuilder::I128(right)) => {
                left.merge(right)
            }
            (
                RuntimeFilterDomainBuilder::Generic(left),
                RuntimeFilterDomainBuilder::Generic(right),
            ) => left.merge(right),
            _ => {
                return Err(paro_error::internal(
                    "hash join runtime filter physical domain changed during merge",
                ));
            }
        }
        Ok(())
    }

    fn freeze(self) -> JoinRuntimeFilterKey {
        JoinRuntimeFilterKey {
            logical_type: self.logical_type,
            non_null_count: self.non_null_count,
            domain: self.domain.freeze(),
        }
    }
}

#[derive(Debug)]
pub struct JoinRuntimeFilter {
    keys: Box<[JoinRuntimeFilterKey]>,
}

impl JoinRuntimeFilter {
    pub(crate) fn predicate_for_column(
        &self,
        build_key_index: usize,
        probe_column_id: ColumnId,
    ) -> Option<PredicateTree> {
        self.keys
            .get(build_key_index)
            .and_then(|key| key.predicate_for_column(probe_column_id))
    }
}

#[derive(Debug)]
struct JoinRuntimeFilterKey {
    logical_type: LogicalType,
    non_null_count: u64,
    domain: RuntimeFilterDomain,
}

impl JoinRuntimeFilterKey {
    fn predicate_for_column(&self, column_id: ColumnId) -> Option<PredicateTree> {
        if self.non_null_count == 0 {
            return None;
        }
        let predicate = match &self.domain {
            RuntimeFilterDomain::I32(domain) => match &self.logical_type {
                LogicalType::Integer => {
                    exact_or_range_predicate(column_id, domain, Value::Integer)?
                }
                LogicalType::Date => exact_or_range_predicate(column_id, domain, Value::Date)?,
                _ => return None,
            },
            RuntimeFilterDomain::I64(domain) => match &self.logical_type {
                LogicalType::BigInt => exact_or_range_predicate(column_id, domain, Value::BigInt)?,
                LogicalType::Decimal { precision, scale } => {
                    exact_or_range_predicate(column_id, domain, |value| {
                        Value::Decimal(value as i128, *precision, *scale)
                    })?
                }
                _ => return None,
            },
            RuntimeFilterDomain::I128(domain) => match &self.logical_type {
                LogicalType::Decimal { precision, scale } => {
                    exact_or_range_predicate(column_id, domain, |value| {
                        Value::Decimal(value, *precision, *scale)
                    })?
                }
                _ => return None,
            },
            RuntimeFilterDomain::Generic(domain) => {
                if !domain.comparable {
                    return None;
                }
                let min = domain.min.clone()?;
                let max = domain.max.clone()?;
                min.partial_cmp(&max)?;
                if min == max {
                    Predicate::Eq {
                        column_id,
                        value: min,
                    }
                } else {
                    Predicate::Range {
                        column_id,
                        lower: min,
                        upper: max,
                    }
                }
            }
        };
        Some(PredicateTree::leaf(predicate))
    }
}

fn visit_fixed<T: Copy>(
    vector: &Vector,
    logical_count: usize,
    selected: &[u32],
    fallback: impl Fn(&Vector, usize) -> Option<T>,
    mut visit: impl FnMut(Option<T>),
) -> Result<()> {
    let view = vector.try_to_view(logical_count)?;
    match view.data() {
        DataRef::Ptr(data) => {
            let data = data.cast::<T>();
            for &row in selected {
                let row_idx = row as usize;
                if !view.is_valid(row_idx) {
                    visit(None);
                    continue;
                }
                let value = unsafe { *data.add(view.physical_index(row_idx)) };
                visit(Some(value));
            }
        }
        DataRef::SequenceI64 { .. } => {
            for &row in selected {
                let row_idx = row as usize;
                if !view.is_valid(row_idx) {
                    visit(None);
                    continue;
                }
                visit(Some(fallback(vector, row_idx).ok_or_else(|| {
                    paro_error::internal("runtime filter sequence value missing")
                })?));
            }
        }
    }
    Ok(())
}

#[inline]
fn add_exact_value<T>(
    domain: &mut ExactDomainBuilder<T>,
    non_null_count: &mut u64,
    value: Option<T>,
) where
    T: Copy + Eq + Ord,
{
    let Some(value) = value else {
        return;
    };
    *non_null_count += 1;
    domain.add(value);
}

fn exact_or_range_predicate<T: Copy + Eq + Ord>(
    column_id: ColumnId,
    domain: &FrozenExactDomain<T>,
    to_value: impl Fn(T) -> Value,
) -> Option<Predicate> {
    let min = domain.min?;
    let max = domain.max?;
    if min == max {
        return Some(Predicate::Eq {
            column_id,
            value: to_value(min),
        });
    }
    if let Some(values) = domain.values.values.as_ref() {
        return Some(Predicate::FixedIn {
            column_id,
            values: values.clone(),
        });
    }
    Some(Predicate::Range {
        column_id,
        lower: to_value(min),
        upper: to_value(max),
    })
}

fn add_generic_vector_value(
    domain: &mut GenericDomain,
    logical_type: &LogicalType,
    vector: &Vector,
    row_idx: usize,
) -> Result<()> {
    let value = match logical_type {
        LogicalType::Boolean => Value::Boolean(required(vector.get_bool(row_idx))?),
        LogicalType::TinyInt => Value::TinyInt(required(vector.get_i8(row_idx))?),
        LogicalType::SmallInt => Value::SmallInt(required(vector.get_i16(row_idx))?),
        LogicalType::HugeInt => Value::HugeInt(required(vector.get_i128(row_idx))?),
        LogicalType::UTinyInt => Value::UTinyInt(required(vector.get_u8(row_idx))?),
        LogicalType::USmallInt => Value::USmallInt(required(vector.get_u16(row_idx))?),
        LogicalType::UInteger => Value::UInteger(required(vector.get_u32(row_idx))?),
        LogicalType::UBigInt => Value::UBigInt(required(vector.get_u64(row_idx))?),
        LogicalType::UHugeInt => Value::UHugeInt(required(vector.get_u128(row_idx))?),
        LogicalType::Uuid => Value::Uuid(required(vector.get_u128(row_idx))?),
        LogicalType::Float => Value::Float(required(vector.get_f32(row_idx))?),
        LogicalType::Double => Value::Double(required(vector.get_f64(row_idx))?),
        LogicalType::Timestamp => Value::Timestamp(required(vector.get_i64(row_idx))?),
        LogicalType::TimestampTz => Value::TimestampTz(required(vector.get_i64(row_idx))?),
        LogicalType::Time => Value::Time(required(vector.get_i64(row_idx))?),
        LogicalType::Varchar
        | LogicalType::VarcharCollation(_)
        | LogicalType::TsVector
        | LogicalType::TsQuery
        | LogicalType::Json
        | LogicalType::Jsonb => {
            domain.add_string(required(vector.get_string(row_idx))?);
            return Ok(());
        }
        _ => {
            domain.comparable = false;
            return Ok(());
        }
    };
    domain.add_value(value);
    Ok(())
}

fn required<T>(value: Option<T>) -> Result<T> {
    value.ok_or_else(|| paro_error::internal("hash join runtime filter value missing"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memory_runtime::QueryMemoryPool;
    use paro_common::test_utils::{test_allocator, test_i64_vector_with_allocator};

    #[test]
    fn typed_exact_domain_freezes_in_order() {
        let allocator = test_allocator();
        let vector = test_i64_vector_with_allocator(&[30, 10, 20, 10], allocator.clone());
        let keys = Chunk::from_arc_vectors(vec![std::sync::Arc::new(vector)], allocator.clone());
        let selection = SelectionVector::try_incremental(4, allocator).unwrap();
        let mut sketch = JoinRuntimeFilterBuilder::empty(&[LogicalType::BigInt]);
        sketch.add_key_chunk(&keys, &selection, 4).unwrap();
        let filter = sketch.freeze();

        assert_eq!(
            filter.predicate_for_column(0, 9),
            Some(PredicateTree::leaf(Predicate::FixedIn {
                column_id: 9,
                values: FixedMembership::i64(vec![10, 20, 30]),
            }))
        );
    }

    #[test]
    fn singleton_exact_domain_publishes_equality() {
        let allocator = test_allocator();
        let vector = test_i64_vector_with_allocator(&[42, 42], allocator.clone());
        let keys = Chunk::from_arc_vectors(vec![std::sync::Arc::new(vector)], allocator.clone());
        let selection = SelectionVector::try_incremental(2, allocator).unwrap();
        let mut builder = JoinRuntimeFilterBuilder::empty(&[LogicalType::BigInt]);
        builder.add_key_chunk(&keys, &selection, 2).unwrap();

        assert_eq!(
            builder.freeze().predicate_for_column(0, 9),
            Some(PredicateTree::leaf(Predicate::Eq {
                column_id: 9,
                value: Value::BigInt(42),
            }))
        );
    }

    #[test]
    fn exact_domain_budget_counts_distinct_values() {
        let allocator = test_allocator();
        let vector =
            test_i64_vector_with_allocator(&[30, 10, 20, 10, 30, 20, 10], allocator.clone());
        let keys = Chunk::from_arc_vectors(vec![std::sync::Arc::new(vector)], allocator.clone());
        let selection = SelectionVector::try_incremental(7, allocator).unwrap();
        let mut builder =
            JoinRuntimeFilterBuilder::empty_with_exact_value_limit(&[LogicalType::BigInt], 3);
        builder.add_key_chunk(&keys, &selection, 7).unwrap();

        assert_eq!(
            builder.freeze().predicate_for_column(0, 9),
            Some(PredicateTree::leaf(Predicate::FixedIn {
                column_id: 9,
                values: FixedMembership::i64(vec![10, 20, 30]),
            }))
        );
    }

    #[test]
    fn exact_domain_degrades_after_distinct_value_budget() {
        let allocator = test_allocator();
        let vector = test_i64_vector_with_allocator(&[30, 10, 20, 40], allocator.clone());
        let keys = Chunk::from_arc_vectors(vec![std::sync::Arc::new(vector)], allocator.clone());
        let selection = SelectionVector::try_incremental(4, allocator).unwrap();
        let mut builder =
            JoinRuntimeFilterBuilder::empty_with_exact_value_limit(&[LogicalType::BigInt], 3);
        builder.add_key_chunk(&keys, &selection, 4).unwrap();

        assert_eq!(
            builder.freeze().predicate_for_column(0, 9),
            Some(PredicateTree::leaf(Predicate::Range {
                column_id: 9,
                lower: Value::BigInt(10),
                upper: Value::BigInt(40),
            }))
        );
    }

    #[test]
    fn exact_domain_low_ndv_buffer_stays_vector_sized() {
        let memory = MemoryAccountingContext::detached(
            MemoryTag::HashTable,
            MemoryAccountingClass::Metadata,
        );
        let max_values = 512 * 1024;
        let mut exact = ExactValues::mutable(memory);
        for _ in 0..32 {
            exact.prepare_batch(VECTOR_SIZE, max_values);
            for row in 0..VECTOR_SIZE {
                exact.insert((row % 10) as i64, max_values);
            }
        }

        let ExactValues::Enabled {
            values,
            canonical_len,
            ..
        } = exact
        else {
            panic!("low-NDV exact domain unexpectedly disabled");
        };
        assert_eq!(canonical_len, 10);
        assert_eq!(values.len(), 10);
        assert!(values.capacity() <= VECTOR_SIZE + 10);
    }

    #[test]
    fn exact_domain_near_budget_accumulates_a_pending_suffix() {
        let memory = MemoryAccountingContext::detached(
            MemoryTag::HashTable,
            MemoryAccountingClass::Metadata,
        );
        let max_values = 64;
        let mut exact = ExactValues::mutable(memory);
        exact.prepare_batch(max_values, max_values);
        for value in 0_i64..63 {
            exact.insert(value, max_values);
        }
        // Reach the first normalization boundary with 63 distinct values.
        exact.insert(0, max_values);
        // A near-full canonical set must buy another O(M) suffix before the
        // next sort instead of sorting the whole domain once per input row.
        for _ in 0..62 {
            exact.insert(0, max_values);
        }

        let ExactValues::Enabled {
            values,
            canonical_len,
            ..
        } = exact
        else {
            panic!("near-budget exact domain unexpectedly disabled");
        };
        assert_eq!(canonical_len, 63);
        assert_eq!(values.len(), 125);
        assert!(values.capacity() <= 126);
    }

    #[test]
    fn exact_domain_degrades_under_query_memory_pressure() {
        let allocator = test_allocator();
        let values = (0_i32..256).map(|value| value * 2).collect::<Vec<_>>();
        let mut vector =
            Vector::try_new(LogicalType::Integer, values.len(), allocator.clone()).unwrap();
        for (row, value) in values.iter().copied().enumerate() {
            vector.set_i32(row, value);
        }
        vector.set_count(values.len());
        let keys = Chunk::from_arc_vectors(vec![std::sync::Arc::new(vector)], allocator.clone());
        let selection = SelectionVector::try_incremental(values.len(), allocator).unwrap();

        let pool = std::sync::Arc::new(QueryMemoryPool::new(64));
        let owner: std::sync::Arc<dyn paro_common::memory::MemoryOwner> = pool;
        let memory = MemoryAccountingContext::from_owner(
            owner,
            paro_common::memory::MemoryDomain::Host,
            MemoryTag::HashTable,
            MemoryAccountingClass::Metadata,
        );
        let mut sketch =
            JoinRuntimeFilterBuilder::empty_with_memory(&[LogicalType::Integer], memory);
        sketch
            .add_key_chunk(&keys, &selection, values.len())
            .unwrap();
        let filter = sketch.freeze();

        assert_eq!(
            filter.predicate_for_column(0, 7),
            Some(PredicateTree::leaf(Predicate::Range {
                column_id: 7,
                lower: Value::Integer(0),
                upper: Value::Integer(510),
            }))
        );
    }
}
