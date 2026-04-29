// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Conservative predicate normalization shared by SSI and predicate locks.

use crate::{ReadDependency, TableId};
use std::collections::BTreeMap;

const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum PredicateValue {
    Null,
    Bool(bool),
    I64(i64),
    U64(u64),
    Bytes(Box<[u8]>),
}

impl PredicateValue {
    #[inline]
    pub fn bytes(value: impl Into<Box<[u8]>>) -> Self {
        Self::Bytes(value.into())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PredicateBound {
    pub value: PredicateValue,
    pub inclusive: bool,
}

impl PredicateBound {
    #[inline]
    pub const fn new(value: PredicateValue, inclusive: bool) -> Self {
        Self { value, inclusive }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum PredicateAtom {
    Equals {
        column_id: u32,
        value: PredicateValue,
    },
    Range {
        column_id: u32,
        lower: Option<PredicateBound>,
        upper: Option<PredicateBound>,
    },
    IsNull {
        column_id: u32,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum PredicateExpr {
    True,
    False,
    Atom(PredicateAtom),
    And(Vec<PredicateExpr>),
    Or(Vec<PredicateExpr>),
    Unsupported,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PredicateFallbackScope {
    Table,
    Tablet { tablet_id: u64 },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NormalizedPredicate {
    table_id: TableId,
    predicate_hash: u64,
    terms: Box<[NormalizedPredicateTerm]>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum NormalizedPredicateTerm {
    Equals {
        column_id: u32,
        value: PredicateValue,
    },
    Range {
        column_id: u32,
        lower: Option<PredicateBound>,
        upper: Option<PredicateBound>,
    },
    IsNull {
        column_id: u32,
    },
    OrBranch {
        predicate_hash: u64,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NormalizedPredicateRead {
    NoRows,
    Exact(NormalizedPredicate),
    Coarse(ReadDependency),
}

#[derive(Debug, Default, Clone, Copy)]
pub struct PredicateNormalizer;

impl PredicateNormalizer {
    pub fn normalize(
        table_id: TableId,
        fallback: PredicateFallbackScope,
        expr: &PredicateExpr,
    ) -> NormalizedPredicateRead {
        match normalize_expr(expr) {
            NormalizedExpr::NoRows => NormalizedPredicateRead::NoRows,
            NormalizedExpr::AllRows => {
                NormalizedPredicateRead::Coarse(fallback_dependency(table_id, fallback))
            }
            NormalizedExpr::Exact(mut terms) => {
                terms.sort();
                terms.dedup();
                let predicate_hash = hash_terms(table_id, &terms);
                NormalizedPredicateRead::Exact(NormalizedPredicate {
                    table_id,
                    predicate_hash,
                    terms: terms.into_boxed_slice(),
                })
            }
            NormalizedExpr::Unsupported => {
                NormalizedPredicateRead::Coarse(fallback_dependency(table_id, fallback))
            }
        }
    }

    #[inline]
    pub fn normalize_to_dependency(
        table_id: TableId,
        fallback: PredicateFallbackScope,
        expr: &PredicateExpr,
    ) -> Option<ReadDependency> {
        match Self::normalize(table_id, fallback, expr) {
            NormalizedPredicateRead::NoRows => None,
            NormalizedPredicateRead::Exact(predicate) => Some(predicate.read_dependency()),
            NormalizedPredicateRead::Coarse(dependency) => Some(dependency),
        }
    }
}

impl NormalizedPredicate {
    #[inline]
    pub const fn table_id(&self) -> TableId {
        self.table_id
    }

    #[inline]
    pub const fn predicate_hash(&self) -> u64 {
        self.predicate_hash
    }

    #[inline]
    pub fn terms(&self) -> &[NormalizedPredicateTerm] {
        &self.terms
    }

    #[inline]
    pub const fn read_dependency(&self) -> ReadDependency {
        ReadDependency::Predicate {
            table_id: self.table_id,
            predicate_hash: self.predicate_hash,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum NormalizedExpr {
    NoRows,
    AllRows,
    Exact(Vec<NormalizedPredicateTerm>),
    Unsupported,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ColumnRange {
    lower: Option<PredicateBound>,
    upper: Option<PredicateBound>,
}

fn normalize_expr(expr: &PredicateExpr) -> NormalizedExpr {
    match expr {
        PredicateExpr::True => NormalizedExpr::AllRows,
        PredicateExpr::False => NormalizedExpr::NoRows,
        PredicateExpr::Atom(atom) => normalize_atom(atom),
        PredicateExpr::And(children) => normalize_and(children),
        PredicateExpr::Or(children) => normalize_or(children),
        PredicateExpr::Unsupported => NormalizedExpr::Unsupported,
    }
}

fn normalize_atom(atom: &PredicateAtom) -> NormalizedExpr {
    match atom {
        PredicateAtom::Equals { column_id, value } => {
            NormalizedExpr::Exact(vec![NormalizedPredicateTerm::Equals {
                column_id: *column_id,
                value: value.clone(),
            }])
        }
        PredicateAtom::Range {
            column_id,
            lower,
            upper,
        } if range_is_contradictory(lower.as_ref(), upper.as_ref()) => NormalizedExpr::NoRows,
        PredicateAtom::Range {
            column_id,
            lower,
            upper,
        } => NormalizedExpr::Exact(vec![NormalizedPredicateTerm::Range {
            column_id: *column_id,
            lower: lower.clone(),
            upper: upper.clone(),
        }]),
        PredicateAtom::IsNull { column_id } => {
            NormalizedExpr::Exact(vec![NormalizedPredicateTerm::IsNull {
                column_id: *column_id,
            }])
        }
    }
}

fn normalize_and(children: &[PredicateExpr]) -> NormalizedExpr {
    let mut terms = Vec::new();
    for child in children {
        match normalize_expr(child) {
            NormalizedExpr::NoRows => return NormalizedExpr::NoRows,
            NormalizedExpr::AllRows => {}
            NormalizedExpr::Exact(child_terms) => terms.extend(child_terms),
            NormalizedExpr::Unsupported => return NormalizedExpr::Unsupported,
        }
    }
    if terms.is_empty() {
        return NormalizedExpr::AllRows;
    }
    merge_conjunctive_terms(terms)
}

fn normalize_or(children: &[PredicateExpr]) -> NormalizedExpr {
    let mut branch_hashes = Vec::new();
    for child in children {
        match normalize_expr(child) {
            NormalizedExpr::NoRows => {}
            NormalizedExpr::AllRows => return NormalizedExpr::AllRows,
            NormalizedExpr::Exact(mut terms) => {
                terms.sort();
                terms.dedup();
                branch_hashes.push(hash_terms(TableId::new(0), &terms));
            }
            NormalizedExpr::Unsupported => return NormalizedExpr::Unsupported,
        }
    }
    if branch_hashes.is_empty() {
        return NormalizedExpr::NoRows;
    }
    branch_hashes.sort_unstable();
    branch_hashes.dedup();
    NormalizedExpr::Exact(
        branch_hashes
            .into_iter()
            .map(|predicate_hash| NormalizedPredicateTerm::OrBranch { predicate_hash })
            .collect(),
    )
}

fn merge_conjunctive_terms(terms: Vec<NormalizedPredicateTerm>) -> NormalizedExpr {
    let mut merged = Vec::new();
    let mut ranges = BTreeMap::<u32, ColumnRange>::new();
    for term in terms {
        match term {
            NormalizedPredicateTerm::Range {
                column_id,
                lower,
                upper,
            } => {
                let range = ranges.entry(column_id).or_insert(ColumnRange {
                    lower: None,
                    upper: None,
                });
                range.lower = strongest_lower(range.lower.take(), lower);
                range.upper = strongest_upper(range.upper.take(), upper);
                if range_is_contradictory(range.lower.as_ref(), range.upper.as_ref()) {
                    return NormalizedExpr::NoRows;
                }
            }
            other => merged.push(other),
        }
    }

    for (column_id, range) in ranges {
        merged.push(NormalizedPredicateTerm::Range {
            column_id,
            lower: range.lower,
            upper: range.upper,
        });
    }
    NormalizedExpr::Exact(merged)
}

fn strongest_lower(
    left: Option<PredicateBound>,
    right: Option<PredicateBound>,
) -> Option<PredicateBound> {
    match (left, right) {
        (None, bound) | (bound, None) => bound,
        (Some(left), Some(right)) => {
            if right.value > left.value || (right.value == left.value && !right.inclusive) {
                Some(right)
            } else {
                Some(left)
            }
        }
    }
}

fn strongest_upper(
    left: Option<PredicateBound>,
    right: Option<PredicateBound>,
) -> Option<PredicateBound> {
    match (left, right) {
        (None, bound) | (bound, None) => bound,
        (Some(left), Some(right)) => {
            if right.value < left.value || (right.value == left.value && !right.inclusive) {
                Some(right)
            } else {
                Some(left)
            }
        }
    }
}

fn range_is_contradictory(lower: Option<&PredicateBound>, upper: Option<&PredicateBound>) -> bool {
    let (Some(lower), Some(upper)) = (lower, upper) else {
        return false;
    };
    lower.value > upper.value
        || (lower.value == upper.value && (!lower.inclusive || !upper.inclusive))
}

fn fallback_dependency(table_id: TableId, fallback: PredicateFallbackScope) -> ReadDependency {
    match fallback {
        PredicateFallbackScope::Table => ReadDependency::Table { table_id },
        PredicateFallbackScope::Tablet { tablet_id } => ReadDependency::Tablet {
            table_id,
            tablet_id,
            read_ts: crate::ReadTs::new(0),
            layout_epoch: 0,
            rowset_count: 0,
        },
    }
}

fn hash_terms(table_id: TableId, terms: &[NormalizedPredicateTerm]) -> u64 {
    let mut hasher = StableHasher::new();
    hasher.write_u8(1);
    hasher.write_u64(table_id.into_raw());
    hasher.write_u64(terms.len() as u64);
    for term in terms {
        hash_term(&mut hasher, term);
    }
    hasher.finish()
}

fn hash_term(hasher: &mut StableHasher, term: &NormalizedPredicateTerm) {
    match term {
        NormalizedPredicateTerm::Equals { column_id, value } => {
            hasher.write_u8(1);
            hasher.write_u64(*column_id as u64);
            hash_value(hasher, value);
        }
        NormalizedPredicateTerm::Range {
            column_id,
            lower,
            upper,
        } => {
            hasher.write_u8(2);
            hasher.write_u64(*column_id as u64);
            hash_bound(hasher, lower.as_ref());
            hash_bound(hasher, upper.as_ref());
        }
        NormalizedPredicateTerm::IsNull { column_id } => {
            hasher.write_u8(3);
            hasher.write_u64(*column_id as u64);
        }
        NormalizedPredicateTerm::OrBranch { predicate_hash } => {
            hasher.write_u8(4);
            hasher.write_u64(*predicate_hash);
        }
    }
}

fn hash_bound(hasher: &mut StableHasher, bound: Option<&PredicateBound>) {
    if let Some(bound) = bound {
        hasher.write_u8(1);
        hasher.write_u8(u8::from(bound.inclusive));
        hash_value(hasher, &bound.value);
    } else {
        hasher.write_u8(0);
    }
}

fn hash_value(hasher: &mut StableHasher, value: &PredicateValue) {
    match value {
        PredicateValue::Null => hasher.write_u8(0),
        PredicateValue::Bool(value) => {
            hasher.write_u8(1);
            hasher.write_u8(u8::from(*value));
        }
        PredicateValue::I64(value) => {
            hasher.write_u8(2);
            hasher.write_u64(*value as u64);
        }
        PredicateValue::U64(value) => {
            hasher.write_u8(3);
            hasher.write_u64(*value);
        }
        PredicateValue::Bytes(value) => {
            hasher.write_u8(4);
            hasher.write_u64(value.len() as u64);
            hasher.write_bytes(value);
        }
    }
}

struct StableHasher(u64);

impl StableHasher {
    const fn new() -> Self {
        Self(FNV_OFFSET)
    }

    fn write_u8(&mut self, value: u8) {
        self.0 ^= u64::from(value);
        self.0 = self.0.wrapping_mul(FNV_PRIME);
    }

    fn write_u64(&mut self, value: u64) {
        for byte in value.to_le_bytes() {
            self.write_u8(byte);
        }
    }

    fn write_bytes(&mut self, bytes: &[u8]) {
        for byte in bytes {
            self.write_u8(*byte);
        }
    }

    const fn finish(self) -> u64 {
        self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ge(column_id: u32, value: i64) -> PredicateExpr {
        PredicateExpr::Atom(PredicateAtom::Range {
            column_id,
            lower: Some(PredicateBound::new(PredicateValue::I64(value), true)),
            upper: None,
        })
    }

    fn lt(column_id: u32, value: i64) -> PredicateExpr {
        PredicateExpr::Atom(PredicateAtom::Range {
            column_id,
            lower: None,
            upper: Some(PredicateBound::new(PredicateValue::I64(value), false)),
        })
    }

    #[test]
    fn and_normalization_sorts_and_merges_ranges() {
        let normalized = PredicateNormalizer::normalize(
            TableId::new(10),
            PredicateFallbackScope::Table,
            &PredicateExpr::And(vec![
                lt(2, 100),
                ge(2, 10),
                PredicateExpr::True,
                PredicateExpr::Atom(PredicateAtom::Equals {
                    column_id: 1,
                    value: PredicateValue::U64(7),
                }),
            ]),
        );

        let NormalizedPredicateRead::Exact(predicate) = normalized else {
            panic!("expected exact normalized predicate");
        };
        assert_eq!(predicate.terms().len(), 2);
        assert_eq!(
            predicate.read_dependency().table_id(),
            Some(TableId::new(10))
        );
    }

    #[test]
    fn contradictory_range_reads_no_rows() {
        let normalized = PredicateNormalizer::normalize(
            TableId::new(10),
            PredicateFallbackScope::Table,
            &PredicateExpr::And(vec![ge(2, 100), lt(2, 10)]),
        );

        assert_eq!(normalized, NormalizedPredicateRead::NoRows);
    }

    #[test]
    fn unsupported_predicate_falls_back_to_table_marker() {
        let normalized = PredicateNormalizer::normalize(
            TableId::new(10),
            PredicateFallbackScope::Table,
            &PredicateExpr::Unsupported,
        );

        assert_eq!(
            normalized,
            NormalizedPredicateRead::Coarse(ReadDependency::Table {
                table_id: TableId::new(10)
            })
        );
    }

    #[test]
    fn or_normalization_is_order_insensitive() {
        let left = PredicateNormalizer::normalize(
            TableId::new(10),
            PredicateFallbackScope::Table,
            &PredicateExpr::Or(vec![ge(2, 10), lt(2, 100)]),
        );
        let right = PredicateNormalizer::normalize(
            TableId::new(10),
            PredicateFallbackScope::Table,
            &PredicateExpr::Or(vec![lt(2, 100), ge(2, 10)]),
        );

        assert_eq!(left, right);
    }
}
