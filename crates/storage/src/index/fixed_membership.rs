// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Immutable physical-value sets for fixed-width storage predicates.
//!
//! Dense domains use a bitset and sparse domains use sorted values. The
//! frozen domain is reference counted because runtime predicates are cloned
//! into independent segment readers after a join build completes. The Arc owns
//! the Vec headers rather than separately reference-counting each slice: freeze
//! can therefore transfer every large backing allocation without copying it.

use std::sync::Arc;

/// Construction limits for the dense representation of a fixed-width set.
///
/// Callers with different access patterns can choose a policy without changing
/// the set's lookup contract. Static predicates use the conservative default;
/// analytical runtime filters may spend more bounded memory to avoid a binary
/// search for every scanned row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FixedMembershipBuildPolicy {
    max_dense_bits: usize,
    expected_probe_count: usize,
}

impl FixedMembershipBuildPolicy {
    pub const fn new(max_dense_bits: usize, expected_probe_count: usize) -> Self {
        Self {
            max_dense_bits,
            expected_probe_count,
        }
    }

    fn permits_dense(self, span: usize, value_count: usize) -> bool {
        if span > self.max_dense_bits || value_count < 2 {
            return false;
        }

        // Compare executable work instead of classifying a domain by an
        // arbitrary span/value ratio. A sorted set needs ceil(log2(N))
        // comparisons for every probe. Dense lookup needs one indexed load,
        // plus one sequential initialization pass over its backing storage.
        // The representation changes only when the predicted lookup savings
        // pay for that initialization within this consumer's scan.
        let sorted_comparisons =
            usize::BITS as usize - value_count.saturating_sub(1).leading_zeros() as usize;
        let lookup_savings = self
            .expected_probe_count
            .saturating_mul(sorted_comparisons.saturating_sub(1));
        let initialization_words = if span <= MAX_BYTE_LOOKUP_DOMAIN {
            span.div_ceil(std::mem::size_of::<u64>())
        } else {
            span.div_ceil(u64::BITS as usize)
        };
        initialization_words <= lookup_savings
    }
}

impl Default for FixedMembershipBuildPolicy {
    fn default() -> Self {
        Self::new(1 << 20, 0)
    }
}

pub(crate) trait FixedMembershipValue: Copy + Ord {
    fn offset_from(self, base: Self) -> Option<usize>;
}

macro_rules! impl_fixed_membership_value {
    ($ty:ty, $unsigned:ty) => {
        impl FixedMembershipValue for $ty {
            #[inline]
            fn offset_from(self, base: Self) -> Option<usize> {
                if self < base {
                    return None;
                }
                // With ordered signed endpoints, wrapping subtraction in the
                // corresponding unsigned domain is exactly their mathematical
                // non-negative distance, including ranges crossing zero.
                let offset = (self as $unsigned).wrapping_sub(base as $unsigned);
                usize::try_from(offset).ok()
            }
        }
    };
}

impl_fixed_membership_value!(i32, u32);
impl_fixed_membership_value!(i64, u64);

impl FixedMembershipValue for i128 {
    #[inline]
    fn offset_from(self, base: Self) -> Option<usize> {
        if self < base {
            return None;
        }
        usize::try_from((self as u128).wrapping_sub(base as u128)).ok()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum FixedMembershipRepresentation<T> {
    Sorted(Vec<T>),
    DenseBits {
        base: T,
        span: usize,
        /// Canonical storage is retained because range and enumeration
        /// consumers need logarithmic/linear-in-N access, independently of
        /// the point-lookup accelerator's domain span.
        ordered: Vec<T>,
        bits: Vec<u64>,
    },
    DenseBytes {
        base: T,
        ordered: Vec<T>,
        present: Vec<u8>,
    },
}

/// Borrowed physical representation used to dispatch vector kernels once per
/// batch instead of once per value.
pub(crate) enum FixedMembershipView<'a, T> {
    Sorted(&'a [T]),
    DenseBits {
        base: T,
        span: usize,
        bits: &'a [u64],
    },
    DenseBytes {
        base: T,
        present: &'a [u8],
    },
}

// A byte-addressed membership table removes the word selection and variable
// bit shift from every scanned row. Keep it bounded to a cache-resident domain;
// larger analytical domains retain the compact bitset representation.
const MAX_BYTE_LOOKUP_DOMAIN: usize = 32 * 1024;

/// Immutable membership set for one physical integer width.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FixedMembershipSet<T> {
    representation: Arc<FixedMembershipRepresentation<T>>,
}

impl<T: FixedMembershipValue> FixedMembershipSet<T> {
    pub(crate) fn from_values(values: Vec<T>) -> Self {
        Self::from_values_with_policy(values, FixedMembershipBuildPolicy::default())
    }

    pub(crate) fn from_values_with_policy(
        mut values: Vec<T>,
        policy: FixedMembershipBuildPolicy,
    ) -> Self {
        if values.is_empty() {
            return Self {
                representation: Arc::new(FixedMembershipRepresentation::Sorted(Vec::new())),
            };
        }

        values.sort_unstable();
        values.dedup();
        let min = values[0];
        let max = values[values.len() - 1];
        if let Some(span) = max
            .offset_from(min)
            .and_then(|offset| offset.checked_add(1))
            .filter(|span| policy.permits_dense(*span, values.len()))
        {
            if span <= MAX_BYTE_LOOKUP_DOMAIN {
                let mut present = vec![0_u8; span];
                for &value in &values {
                    let offset = value
                        .offset_from(min)
                        .expect("membership value lies inside measured byte lookup range");
                    present[offset] = 1;
                }
                return Self {
                    representation: Arc::new(FixedMembershipRepresentation::DenseBytes {
                        base: min,
                        ordered: values,
                        present,
                    }),
                };
            }
            let mut bits = vec![0_u64; span.div_ceil(u64::BITS as usize)];
            for &value in &values {
                let offset = value
                    .offset_from(min)
                    .expect("membership value lies inside measured dense range");
                let word = &mut bits[offset / u64::BITS as usize];
                let mask = 1_u64 << (offset % u64::BITS as usize);
                *word |= mask;
            }
            return Self {
                representation: Arc::new(FixedMembershipRepresentation::DenseBits {
                    base: min,
                    span,
                    ordered: values,
                    bits,
                }),
            };
        }

        Self {
            representation: Arc::new(FixedMembershipRepresentation::Sorted(values)),
        }
    }

    #[inline]
    pub(crate) fn contains(&self, value: T) -> bool {
        match self.representation.as_ref() {
            FixedMembershipRepresentation::Sorted(values) => values.binary_search(&value).is_ok(),
            FixedMembershipRepresentation::DenseBits {
                base, span, bits, ..
            } => {
                let Some(offset) = value.offset_from(*base).filter(|offset| *offset < *span) else {
                    return false;
                };
                bits[offset / u64::BITS as usize] & (1_u64 << (offset % u64::BITS as usize)) != 0
            }
            FixedMembershipRepresentation::DenseBytes { base, present, .. } => value
                .offset_from(*base)
                .and_then(|offset| present.get(offset))
                .is_some_and(|present| *present != 0),
        }
    }

    pub(crate) fn view(&self) -> FixedMembershipView<'_, T> {
        match self.representation.as_ref() {
            FixedMembershipRepresentation::Sorted(values) => FixedMembershipView::Sorted(values),
            FixedMembershipRepresentation::DenseBits {
                base, span, bits, ..
            } => FixedMembershipView::DenseBits {
                base: *base,
                span: *span,
                bits,
            },
            FixedMembershipRepresentation::DenseBytes { base, present, .. } => {
                FixedMembershipView::DenseBytes {
                    base: *base,
                    present,
                }
            }
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.canonical_values().len()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn allocation_size(&self) -> usize {
        let payload = match self.representation.as_ref() {
            FixedMembershipRepresentation::Sorted(values) => {
                values.capacity().saturating_mul(std::mem::size_of::<T>())
            }
            FixedMembershipRepresentation::DenseBits { ordered, bits, .. } => ordered
                .capacity()
                .saturating_mul(std::mem::size_of::<T>())
                .saturating_add(bits.capacity().saturating_mul(std::mem::size_of::<u64>())),
            FixedMembershipRepresentation::DenseBytes {
                ordered, present, ..
            } => ordered
                .capacity()
                .saturating_mul(std::mem::size_of::<T>())
                .saturating_add(present.capacity()),
        };
        // Count the Arc allocation as retained state as well as the moved Vec
        // backings. Two reference counters precede the payload in Arc's heap
        // allocation; allocator padding can only make this conservative for
        // the memory accounting callers that consume this value.
        payload
            .saturating_add(std::mem::size_of::<FixedMembershipRepresentation<T>>())
            .saturating_add(2 * std::mem::size_of::<usize>())
    }

    pub(crate) fn is_contiguous(&self) -> bool {
        let values = self.canonical_values();
        let (Some(first), Some(last)) = (values.first(), values.last()) else {
            return false;
        };
        last.offset_from(*first)
            .and_then(|offset| offset.checked_add(1))
            == Some(values.len())
    }

    pub(crate) fn first(&self) -> Option<T> {
        self.canonical_values().first().copied()
    }

    pub(crate) fn last(&self) -> Option<T> {
        self.canonical_values().last().copied()
    }

    fn first_at_or_after(&self, target: T) -> Option<T> {
        let values = self.canonical_values();
        values
            .get(values.partition_point(|value| *value < target))
            .copied()
    }

    pub(crate) fn retain(&mut self, mut predicate: impl FnMut(T) -> bool) {
        let values = self
            .iter()
            .filter(|value| predicate(*value))
            .collect::<Vec<_>>();
        *self = Self::from_values(values);
    }

    pub(crate) fn intersect(&mut self, other: &Self) {
        self.retain(|value| other.contains(value));
    }

    fn canonical_values(&self) -> &[T] {
        match self.representation.as_ref() {
            FixedMembershipRepresentation::Sorted(values)
            | FixedMembershipRepresentation::DenseBits {
                ordered: values, ..
            }
            | FixedMembershipRepresentation::DenseBytes {
                ordered: values, ..
            } => values,
        }
    }

    fn iter(&self) -> impl Iterator<Item = T> + '_ {
        self.canonical_values().iter().copied()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum FixedMembershipKind {
    I32(FixedMembershipSet<i32>),
    I64(FixedMembershipSet<i64>),
    I128(FixedMembershipSet<i128>),
}

/// Logical width of a type-erased fixed membership set.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FixedMembershipWidth {
    I32,
    I64,
    I128,
}

/// Type-erased fixed-width membership used by [`super::Predicate`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FixedMembership {
    kind: FixedMembershipKind,
}

impl FixedMembership {
    pub fn i32(values: Vec<i32>) -> Self {
        Self {
            kind: FixedMembershipKind::I32(FixedMembershipSet::from_values(values)),
        }
    }

    pub fn i64(values: Vec<i64>) -> Self {
        Self {
            kind: FixedMembershipKind::I64(FixedMembershipSet::from_values(values)),
        }
    }

    pub fn i128(values: Vec<i128>) -> Self {
        Self {
            kind: FixedMembershipKind::I128(FixedMembershipSet::from_values(values)),
        }
    }

    pub fn i32_with_policy(values: Vec<i32>, policy: FixedMembershipBuildPolicy) -> Self {
        Self {
            kind: FixedMembershipKind::I32(FixedMembershipSet::from_values_with_policy(
                values, policy,
            )),
        }
    }

    pub fn i64_with_policy(values: Vec<i64>, policy: FixedMembershipBuildPolicy) -> Self {
        Self {
            kind: FixedMembershipKind::I64(FixedMembershipSet::from_values_with_policy(
                values, policy,
            )),
        }
    }

    pub fn i128_with_policy(values: Vec<i128>, policy: FixedMembershipBuildPolicy) -> Self {
        Self {
            kind: FixedMembershipKind::I128(FixedMembershipSet::from_values_with_policy(
                values, policy,
            )),
        }
    }

    pub fn len(&self) -> usize {
        match &self.kind {
            FixedMembershipKind::I32(values) => values.len(),
            FixedMembershipKind::I64(values) => values.len(),
            FixedMembershipKind::I128(values) => values.len(),
        }
    }

    /// Heap bytes retained by the immutable membership representation.
    pub fn allocation_size(&self) -> usize {
        match &self.kind {
            FixedMembershipKind::I32(values) => values.allocation_size(),
            FixedMembershipKind::I64(values) => values.allocation_size(),
            FixedMembershipKind::I128(values) => values.allocation_size(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn is_contiguous(&self) -> bool {
        match &self.kind {
            FixedMembershipKind::I32(values) => values.is_contiguous(),
            FixedMembershipKind::I64(values) => values.is_contiguous(),
            FixedMembershipKind::I128(values) => values.is_contiguous(),
        }
    }

    pub fn width(&self) -> FixedMembershipWidth {
        match &self.kind {
            FixedMembershipKind::I32(_) => FixedMembershipWidth::I32,
            FixedMembershipKind::I64(_) => FixedMembershipWidth::I64,
            FixedMembershipKind::I128(_) => FixedMembershipWidth::I128,
        }
    }

    /// Least canonical member greater than or equal to `target`.
    ///
    /// This is the type-erased range-probe primitive used by scalar indexes;
    /// it preserves the frozen dense/sorted representation instead of
    /// rebuilding a boxed byte vector for every segment.
    pub fn first_at_or_after(&self, target: i128) -> Option<i128> {
        match &self.kind {
            FixedMembershipKind::I32(values) => {
                if target <= i128::from(i32::MIN) {
                    values.first().map(i128::from)
                } else if target > i128::from(i32::MAX) {
                    None
                } else {
                    values.first_at_or_after(target as i32).map(i128::from)
                }
            }
            FixedMembershipKind::I64(values) => {
                if target <= i128::from(i64::MIN) {
                    values.first().map(i128::from)
                } else if target > i128::from(i64::MAX) {
                    None
                } else {
                    values.first_at_or_after(target as i64).map(i128::from)
                }
            }
            FixedMembershipKind::I128(values) => values.first_at_or_after(target),
        }
    }

    pub fn first_canonical(&self) -> Option<i128> {
        match &self.kind {
            FixedMembershipKind::I32(values) => values.first().map(i128::from),
            FixedMembershipKind::I64(values) => values.first().map(i128::from),
            FixedMembershipKind::I128(values) => values.first(),
        }
    }

    pub fn last_canonical(&self) -> Option<i128> {
        match &self.kind {
            FixedMembershipKind::I32(values) => values.last().map(i128::from),
            FixedMembershipKind::I64(values) => values.last().map(i128::from),
            FixedMembershipKind::I128(values) => values.last(),
        }
    }

    /// Visit the canonical ascending, deduplicated values independently of the
    /// dense or sorted runtime representation chosen for lookup.
    pub fn visit_canonical_values(&self, mut visit: impl FnMut(i128)) -> FixedMembershipWidth {
        match &self.kind {
            FixedMembershipKind::I32(values) => {
                values.iter().for_each(|value| visit(i128::from(value)));
                FixedMembershipWidth::I32
            }
            FixedMembershipKind::I64(values) => {
                values.iter().for_each(|value| visit(i128::from(value)));
                FixedMembershipWidth::I64
            }
            FixedMembershipKind::I128(values) => {
                values.iter().for_each(&mut visit);
                FixedMembershipWidth::I128
            }
        }
    }

    pub(crate) fn into_kind(self) -> FixedMembershipKind {
        self.kind
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dense_membership_deduplicates_and_iterates_in_order() {
        let mut values = FixedMembershipSet::from_values(vec![12_i32, 10, 12, 15]);
        assert_eq!(values.len(), 3);
        assert_eq!(values.first(), Some(10));
        assert_eq!(values.last(), Some(15));
        assert!(values.contains(12));
        assert!(!values.contains(11));
        assert_eq!(values.iter().collect::<Vec<_>>(), vec![10, 12, 15]);

        values.retain(|value| value >= 12);
        assert_eq!(values.iter().collect::<Vec<_>>(), vec![12, 15]);
    }

    #[test]
    fn sparse_membership_uses_the_same_set_contract() {
        let mut values = FixedMembershipSet::from_values(vec![0_i64, 1_000_000_000, -5]);
        assert_eq!(values.first(), Some(-5));
        assert_eq!(values.last(), Some(1_000_000_000));
        assert!(values.contains(0));
        values.intersect(&FixedMembershipSet::from_values(vec![-5, 7]));
        assert_eq!(values.iter().collect::<Vec<_>>(), vec![-5]);
    }

    #[test]
    fn physical_offsets_are_exact_across_signed_boundaries() {
        assert_eq!(5_i32.offset_from(-5), Some(10));
        assert_eq!((-5_i32).offset_from(5), None);
        assert_eq!(i32::MAX.offset_from(i32::MIN), Some(u32::MAX as usize));
        assert_eq!(5_i64.offset_from(-5), Some(10));
        assert_eq!((-5_i64).offset_from(5), None);
        assert_eq!(5_i128.offset_from(-5), Some(10));
    }

    #[test]
    fn construction_policy_selects_representation_without_changing_contract() {
        let source = vec![0_i64, 128, 256];
        let conservative = FixedMembershipSet::from_values(source.clone());
        let analytical = FixedMembershipSet::from_values_with_policy(
            source,
            FixedMembershipBuildPolicy::new(512, 256),
        );

        assert!(matches!(
            conservative.representation.as_ref(),
            FixedMembershipRepresentation::Sorted(_)
        ));
        assert!(matches!(
            analytical.representation.as_ref(),
            FixedMembershipRepresentation::DenseBytes { .. }
        ));
        assert_eq!(analytical.first_at_or_after(-1), Some(0));
        assert_eq!(analytical.first_at_or_after(1), Some(128));
        assert_eq!(analytical.first_at_or_after(129), Some(256));
        assert_eq!(analytical.first_at_or_after(257), None);
        assert_eq!(
            conservative.iter().collect::<Vec<_>>(),
            analytical.iter().collect::<Vec<_>>()
        );

        let dense_bits = FixedMembershipSet::from_values_with_policy(
            vec![0_i64, 65_536, 131_072],
            FixedMembershipBuildPolicy::new(262_144, 16_384),
        );
        assert!(matches!(
            dense_bits.representation.as_ref(),
            FixedMembershipRepresentation::DenseBits { .. }
        ));
        assert_eq!(dense_bits.first_at_or_after(65_537), Some(131_072));
        assert_eq!(dense_bits.first_at_or_after(131_073), None);
    }

    #[test]
    fn sparse_dense_accelerator_retains_logarithmic_interval_index() {
        let values = FixedMembershipSet::from_values_with_policy(
            vec![0_i64, 33_554_432, 67_108_863],
            FixedMembershipBuildPolicy::new(67_108_864, 2_000_000),
        );

        let FixedMembershipRepresentation::DenseBits { ordered, bits, .. } =
            values.representation.as_ref()
        else {
            panic!("expected point-lookup accelerator");
        };
        assert_eq!(ordered.as_slice(), &[0, 33_554_432, 67_108_863]);
        assert_eq!(bits.len(), 67_108_864 / u64::BITS as usize);
        assert_eq!(values.first_at_or_after(1), Some(33_554_432));
        assert_eq!(values.first_at_or_after(33_554_433), Some(67_108_863));
    }

    #[test]
    fn freeze_transfers_the_canonical_backing_allocation() {
        let input = (0_i32..524_288).collect::<Vec<_>>();
        let input_pointer = input.as_ptr();
        let input_capacity = input.capacity();

        let values = FixedMembershipSet::from_values_with_policy(
            input,
            FixedMembershipBuildPolicy::new(67_108_864, 2_000_000),
        );

        assert_eq!(values.canonical_values().as_ptr(), input_pointer);
        assert_eq!(values.canonical_values().len(), input_capacity);
        assert!(matches!(
            values.representation.as_ref(),
            FixedMembershipRepresentation::DenseBits { .. }
        ));
    }
}
