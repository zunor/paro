// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Build-side domains used to create hash-join runtime filters.
//!
//! Exact fixed-width domains stay in their physical representation while a
//! join is built. They are merged by ownership and frozen into sorted arrays
//! before publication. Generic values retain only min/max statistics. This
//! keeps boxed [`Value`] conversion at the storage-predicate boundary rather
//! than on every build row.

use std::collections::HashSet;
use std::hash::Hash;

use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::hash::ExecutionHashBuilder;
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use paro_common::vector::{DataRef, SelectionVector, Vector};
use paro_storage::index::{ColumnId, FixedMembership, Predicate, PredicateTree};

/// Maximum number of build-side values retained for an exact runtime filter.
///
/// Beyond this bound the domain retains only min/max. This avoids duplicating
/// an unbounded fraction of a join table while preserving exact filters for
/// selective dimensions.
pub(crate) const HASH_JOIN_RUNTIME_FILTER_MAX_VALUES: usize = 32 * 1024;

type ExecutionHashSet<T> = HashSet<T, ExecutionHashBuilder>;

#[derive(Debug)]
enum ExactValues<T> {
    Mutable(ExecutionHashSet<T>),
    Frozen(FixedMembership),
    Disabled,
}

impl<T> ExactValues<T>
where
    T: Copy + Eq + Hash + Ord,
{
    fn mutable() -> Self {
        Self::Mutable(ExecutionHashSet::with_hasher(ExecutionHashBuilder))
    }

    fn insert(&mut self, value: T) {
        let Self::Mutable(values) = self else {
            return;
        };
        if values.len() == HASH_JOIN_RUNTIME_FILTER_MAX_VALUES && !values.contains(&value) {
            *self = Self::Disabled;
            return;
        }
        values.insert(value);
    }

    fn merge(&mut self, incoming: Self) {
        match incoming {
            Self::Disabled => *self = Self::Disabled,
            Self::Frozen(_) => {
                debug_assert!(false, "published exact runtime filter cannot be merged");
                *self = Self::Disabled;
            }
            Self::Mutable(mut incoming) => {
                let Self::Mutable(values) = self else {
                    return;
                };
                if incoming.len() > values.len() {
                    std::mem::swap(values, &mut incoming);
                }
                for value in incoming {
                    if values.len() == HASH_JOIN_RUNTIME_FILTER_MAX_VALUES
                        && !values.contains(&value)
                    {
                        *self = Self::Disabled;
                        return;
                    }
                    values.insert(value);
                }
            }
        }
    }

    fn freeze_with(&mut self, freeze: impl FnOnce(Vec<T>) -> FixedMembership) {
        let Self::Mutable(values) = std::mem::replace(self, Self::Disabled) else {
            return;
        };
        *self = Self::Frozen(freeze(values.into_iter().collect()));
    }

    fn frozen(&self) -> Option<&FixedMembership> {
        match self {
            Self::Frozen(values) => Some(values),
            Self::Mutable(_) | Self::Disabled => None,
        }
    }
}

#[derive(Debug)]
struct ExactDomain<T> {
    min: Option<T>,
    max: Option<T>,
    values: ExactValues<T>,
}

impl<T> ExactDomain<T>
where
    T: Copy + Eq + Hash + Ord,
{
    fn new() -> Self {
        Self {
            min: None,
            max: None,
            values: ExactValues::mutable(),
        }
    }

    #[inline]
    fn add(&mut self, value: T) {
        self.min = Some(self.min.map_or(value, |current| current.min(value)));
        self.max = Some(self.max.map_or(value, |current| current.max(value)));
        self.values.insert(value);
    }

    fn merge(&mut self, incoming: Self) {
        if let Some(value) = incoming.min {
            self.min = Some(self.min.map_or(value, |current| current.min(value)));
        }
        if let Some(value) = incoming.max {
            self.max = Some(self.max.map_or(value, |current| current.max(value)));
        }
        self.values.merge(incoming.values);
    }

    fn freeze_with(&mut self, freeze: impl FnOnce(Vec<T>) -> FixedMembership) {
        self.values.freeze_with(freeze);
    }
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
enum RuntimeFilterDomain {
    I32(ExactDomain<i32>),
    I64(ExactDomain<i64>),
    I128(ExactDomain<i128>),
    Generic(GenericDomain),
}

impl RuntimeFilterDomain {
    fn for_type(logical_type: &LogicalType) -> Self {
        match logical_type {
            LogicalType::Integer | LogicalType::Date => Self::I32(ExactDomain::new()),
            LogicalType::BigInt
            | LogicalType::Decimal {
                precision: 0..=18, ..
            } => Self::I64(ExactDomain::new()),
            LogicalType::Decimal { .. } => Self::I128(ExactDomain::new()),
            _ => Self::Generic(GenericDomain::new()),
        }
    }

    fn freeze(&mut self) {
        match self {
            Self::I32(domain) => domain.freeze_with(FixedMembership::i32),
            Self::I64(domain) => domain.freeze_with(FixedMembership::i64),
            Self::I128(domain) => domain.freeze_with(FixedMembership::i128),
            Self::Generic(_) => {}
        }
    }
}

#[derive(Debug)]
pub struct JoinRuntimeFilterSketch {
    pub row_count: u64,
    pub keys: Box<[JoinRuntimeFilterKeySketch]>,
}

impl JoinRuntimeFilterSketch {
    pub fn empty(key_types: &[LogicalType]) -> Self {
        Self {
            row_count: 0,
            keys: key_types
                .iter()
                .cloned()
                .map(JoinRuntimeFilterKeySketch::new)
                .collect::<Vec<_>>()
                .into_boxed_slice(),
        }
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
        self.row_count += selected_count as u64;
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
        self.row_count += incoming.row_count;
        for (left, right) in self.keys.iter_mut().zip(incoming.keys.into_vec()) {
            left.merge(right)?;
        }
        Ok(())
    }

    pub(crate) fn freeze(&mut self) {
        for key in &mut self.keys {
            key.domain.freeze();
        }
    }

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
pub struct JoinRuntimeFilterKeySketch {
    pub logical_type: LogicalType,
    pub non_null_count: u64,
    pub has_null: bool,
    domain: RuntimeFilterDomain,
}

impl JoinRuntimeFilterKeySketch {
    fn new(logical_type: LogicalType) -> Self {
        Self {
            domain: RuntimeFilterDomain::for_type(&logical_type),
            logical_type,
            non_null_count: 0,
            has_null: false,
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
            RuntimeFilterDomain::I32(domain) => {
                visit_fixed(vector, logical_count, selected, Vector::get_i32, |value| {
                    add_exact_value(domain, &mut self.non_null_count, &mut self.has_null, value)
                })
            }
            RuntimeFilterDomain::I64(domain) => {
                visit_fixed(vector, logical_count, selected, Vector::get_i64, |value| {
                    add_exact_value(domain, &mut self.non_null_count, &mut self.has_null, value)
                })
            }
            RuntimeFilterDomain::I128(domain) => {
                visit_fixed(vector, logical_count, selected, Vector::get_i128, |value| {
                    add_exact_value(domain, &mut self.non_null_count, &mut self.has_null, value)
                })
            }
            RuntimeFilterDomain::Generic(domain) => {
                for &row in selected {
                    let row_idx = row as usize;
                    if vector.is_null(row_idx) {
                        self.has_null = true;
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
        self.has_null |= incoming.has_null;
        self.non_null_count += incoming.non_null_count;
        match (&mut self.domain, incoming.domain) {
            (RuntimeFilterDomain::I32(left), RuntimeFilterDomain::I32(right)) => left.merge(right),
            (RuntimeFilterDomain::I64(left), RuntimeFilterDomain::I64(right)) => left.merge(right),
            (RuntimeFilterDomain::I128(left), RuntimeFilterDomain::I128(right)) => {
                left.merge(right)
            }
            (RuntimeFilterDomain::Generic(left), RuntimeFilterDomain::Generic(right)) => {
                left.merge(right)
            }
            _ => {
                return Err(paro_error::internal(
                    "hash join runtime filter physical domain changed during merge",
                ));
            }
        }
        Ok(())
    }

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
    domain: &mut ExactDomain<T>,
    non_null_count: &mut u64,
    has_null: &mut bool,
    value: Option<T>,
) where
    T: Copy + Eq + Hash + Ord,
{
    let Some(value) = value else {
        *has_null = true;
        return;
    };
    *non_null_count += 1;
    domain.add(value);
}

fn exact_or_range_predicate<T: Copy + Eq + Hash + Ord>(
    column_id: ColumnId,
    domain: &ExactDomain<T>,
    to_value: impl Fn(T) -> Value,
) -> Option<Predicate> {
    let min = domain.min?;
    let max = domain.max?;
    if let Some(values) = domain.values.frozen() {
        if min == max {
            return Some(Predicate::Eq {
                column_id,
                value: to_value(min),
            });
        }
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
    use paro_common::test_utils::{test_allocator, test_i64_vector_with_allocator};

    #[test]
    fn typed_exact_domain_freezes_in_order() {
        let allocator = test_allocator();
        let vector = test_i64_vector_with_allocator(&[30, 10, 20, 10], allocator.clone());
        let keys = Chunk::from_arc_vectors(vec![std::sync::Arc::new(vector)], allocator.clone());
        let selection = SelectionVector::try_incremental(4, allocator).unwrap();
        let mut sketch = JoinRuntimeFilterSketch::empty(&[LogicalType::BigInt]);
        sketch.add_key_chunk(&keys, &selection, 4).unwrap();
        sketch.freeze();

        assert_eq!(
            sketch.predicate_for_column(0, 9),
            Some(PredicateTree::leaf(Predicate::FixedIn {
                column_id: 9,
                values: FixedMembership::i64(vec![10, 20, 30]),
            }))
        );
    }
}
