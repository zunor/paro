// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Ordered, lossless storage-predicate encoding. Never use EXPLAIN text here:
//! display formatting has depth/length limits and summarizes membership sets.

use super::StableFingerprintBuilder;
use paro_storage::index::{Predicate, PredicateComparison, PredicateTree};

pub(super) fn encode_predicate<V>(
    builder: &mut StableFingerprintBuilder,
    tree: &PredicateTree<V>,
    mut encode_value: impl FnMut(&mut StableFingerprintBuilder, &V),
) {
    let mut pending = vec![tree];
    while let Some(tree) = pending.pop() {
        match tree {
            PredicateTree::And(children) | PredicateTree::Or(children) => {
                builder.write_u64(if matches!(tree, PredicateTree::And(_)) {
                    0
                } else {
                    1
                });
                builder.write_u64(children.len() as u64);
                pending.extend(children.iter().rev());
            }
            PredicateTree::Leaf(predicate) => {
                builder.write_u64(2);
                match predicate {
                    Predicate::Eq { column_id, value }
                    | Predicate::NotEq { column_id, value }
                    | Predicate::Lt { column_id, value }
                    | Predicate::Le { column_id, value }
                    | Predicate::Gt { column_id, value }
                    | Predicate::Ge { column_id, value } => {
                        builder.write_u64(match predicate {
                            Predicate::Eq { .. } => 0,
                            Predicate::NotEq { .. } => 1,
                            Predicate::Lt { .. } => 2,
                            Predicate::Le { .. } => 3,
                            Predicate::Gt { .. } => 4,
                            Predicate::Ge { .. } => 5,
                            _ => unreachable!("comparison arm"),
                        });
                        builder.write_u64(u64::from(*column_id));
                        encode_value(builder, value);
                    }
                    Predicate::In { column_id, values } => {
                        builder.write_u64(6);
                        builder.write_u64(u64::from(*column_id));
                        builder.write_u64(values.len() as u64);
                        for value in values {
                            encode_value(builder, value);
                        }
                    }
                    Predicate::FixedIn { column_id, values } => {
                        use paro_storage::index::FixedMembershipWidth;
                        builder.write_u64(7);
                        builder.write_u64(u64::from(*column_id));
                        builder.write_u64(match values.width() {
                            FixedMembershipWidth::I32 => 0,
                            FixedMembershipWidth::I64 => 1,
                            FixedMembershipWidth::I128 => 2,
                        });
                        builder.write_u64(values.len() as u64);
                        values.visit_canonical_values(|value| {
                            builder.write_bytes(&value.to_le_bytes())
                        });
                    }
                    Predicate::Range {
                        column_id,
                        lower,
                        upper,
                    } => {
                        builder.write_u64(8);
                        builder.write_u64(u64::from(*column_id));
                        encode_value(builder, lower);
                        encode_value(builder, upper);
                    }
                    Predicate::IsNull { column_id } | Predicate::IsNotNull { column_id } => {
                        builder.write_u64(if matches!(predicate, Predicate::IsNull { .. }) {
                            9
                        } else {
                            10
                        });
                        builder.write_u64(u64::from(*column_id));
                    }
                    Predicate::StringPrefix {
                        column_id,
                        prefix,
                        negated,
                    } => {
                        builder.write_u64(11);
                        builder.write_u64(u64::from(*column_id));
                        builder.write_bytes(prefix.as_bytes());
                        builder.write_u64(u64::from(*negated));
                    }
                    Predicate::StringPrefixIn {
                        column_id,
                        prefixes,
                    } => {
                        builder.write_u64(12);
                        builder.write_u64(u64::from(*column_id));
                        builder.write_u64(prefixes.len() as u64);
                        for prefix in prefixes {
                            builder.write_bytes(prefix.as_bytes());
                        }
                    }
                    Predicate::StringLike {
                        column_id,
                        pattern,
                        negated,
                    } => {
                        builder.write_u64(13);
                        builder.write_u64(u64::from(*column_id));
                        builder.write_bytes(pattern.as_bytes());
                        builder.write_u64(u64::from(*negated));
                    }
                    Predicate::ColumnComparison {
                        left_column_id,
                        right_column_id,
                        comparison,
                    } => {
                        builder.write_u64(14);
                        builder.write_u64(u64::from(*left_column_id));
                        builder.write_u64(u64::from(*right_column_id));
                        builder.write_u64(match comparison {
                            PredicateComparison::Equal => 0,
                            PredicateComparison::NotEqual => 1,
                            PredicateComparison::LessThan => 2,
                            PredicateComparison::LessThanOrEqual => 3,
                            PredicateComparison::GreaterThan => 4,
                            PredicateComparison::GreaterThanOrEqual => 5,
                        });
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use paro_common::runtime_value::Value;
    use paro_storage::index::FixedMembership;

    fn fingerprint(tree: &PredicateTree) -> super::super::Fingerprint {
        let mut builder = StableFingerprintBuilder::default();
        encode_predicate(
            &mut builder,
            tree,
            crate::physical::scalar_identity::encode_value,
        );
        builder.finish()
    }

    #[test]
    fn membership_values_not_just_count_participate() {
        let predicate = |values| {
            PredicateTree::Leaf(Predicate::FixedIn {
                column_id: 0,
                values: FixedMembership::i64(values),
            })
        };
        assert_ne!(
            fingerprint(&predicate(vec![1, 2])),
            fingerprint(&predicate(vec![3, 4]))
        );
        assert_eq!(
            fingerprint(&predicate(vec![2, 1])),
            fingerprint(&predicate(vec![1, 2]))
        );
    }

    #[test]
    fn values_beyond_display_limit_and_child_order_participate() {
        let leaf = |value| {
            PredicateTree::Leaf(Predicate::Eq {
                column_id: 0,
                value: Value::Integer(value),
            })
        };
        let left = PredicateTree::And((0..2048).map(leaf).collect());
        let mut right = left.clone();
        let PredicateTree::And(children) = &mut right else {
            unreachable!()
        };
        children[2047] = leaf(-1);
        assert_ne!(fingerprint(&left), fingerprint(&right));
        assert_ne!(
            fingerprint(&PredicateTree::And(vec![leaf(1), leaf(2)])),
            fingerprint(&PredicateTree::And(vec![leaf(2), leaf(1)]))
        );
    }
}
