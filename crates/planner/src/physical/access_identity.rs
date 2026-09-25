// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Canonical access-request encoding, shared by logical and physical identities.

use super::identity::StableFingerprintBuilder;

pub fn encode_hnsw_options(
    fingerprint: &mut StableFingerprintBuilder,
    options: paro_storage::index::hnsw::HnswQueryOptions,
) {
    encode_optional_usize(fingerprint, options.ef);
    encode_optional_usize(fingerprint, options.rerank_window);
    fingerprint.write_u64(match options.objective {
        paro_storage::index::hnsw::HnswSearchObjective::CostOptimized => 0,
        paro_storage::index::hnsw::HnswSearchObjective::Exact => 1,
    });
}

pub fn encode_search_request(
    fingerprint: &mut StableFingerprintBuilder,
    request: &paro_storage::search::NormalizedSearchRequest,
) {
    use paro_storage::search::{DenseVectorQuery, FusionStrategy, SearchIntent, SearchRequestMode};

    fingerprint.write_u64(request.table_id);
    match request.mode {
        SearchRequestMode::TopK { limit } => {
            fingerprint.write_u64(0);
            fingerprint.write_u64(limit as u64);
        }
        SearchRequestMode::Filter => fingerprint.write_u64(1),
    }
    match &request.predicate {
        None => fingerprint.write_u64(0),
        Some(predicate) => {
            fingerprint.write_u64(1);
            encode_predicate_tree(fingerprint, predicate);
        }
    }
    encode_u32s(fingerprint, &request.projections.columns);
    fingerprint.write_u64(request.projections.include_score as u64);
    fingerprint.write_u64(request.intents.len() as u64);
    for intent in &request.intents {
        match intent {
            SearchIntent::Hnsw(intent) => {
                fingerprint.write_u64(0);
                fingerprint.write_u64(intent.column_id as u64);
                match &intent.query {
                    DenseVectorQuery::Literal(values) => {
                        fingerprint.write_u64(0);
                        fingerprint.write_u64(values.len() as u64);
                        for value in values {
                            fingerprint.write_u64(value.to_bits() as u64);
                        }
                    }
                    DenseVectorQuery::RuntimeParameter { slot, dimension } => {
                        fingerprint.write_u64(1);
                        fingerprint.write_u64(slot.index.index() as u64);
                        super::scalar_identity::encode_logical_type(fingerprint, &slot.ty);
                        fingerprint.write_u64(*dimension as u64);
                    }
                }
                fingerprint.write_u64(intent.distance as u64);
                encode_hnsw_options(fingerprint, intent.options);
            }
            SearchIntent::Sparse(intent) => {
                fingerprint.write_u64(1);
                fingerprint.write_u64(intent.column_id as u64);
                encode_u32s(fingerprint, &intent.query_vector.dims);
                fingerprint.write_u64(intent.query_vector.weights.len() as u64);
                for value in &intent.query_vector.weights {
                    fingerprint.write_u64(value.to_bits() as u64);
                }
            }
            SearchIntent::FullText(intent) => {
                fingerprint.write_u64(2);
                fingerprint.write_u64(intent.column_id as u64);
                fingerprint.write_bytes(intent.query.as_bytes());
                fingerprint.write_u64(intent.query_kind as u64);
                fingerprint.write_u64(intent.query_stats.term_count as u64);
                fingerprint.write_u64(intent.query_stats.positive_term_count as u64);
                fingerprint.write_u64(intent.query_stats.phrase_count as u64);
                fingerprint.write_u64(intent.query_stats.proximity_count as u64);
                fingerprint.write_u64(intent.query_stats.prefix_count as u64);
                fingerprint.write_u64(intent.query_stats.not_count as u64);
                fingerprint.write_u64(intent.query_stats.or_branch_count as u64);
                fingerprint.write_bytes(intent.config.as_bytes());
                fingerprint.write_u64(intent.score_mode as u64);
            }
        }
    }
    match &request.fusion {
        None => fingerprint.write_u64(0),
        Some(FusionStrategy::ReciprocalRankFusion {
            window_size,
            rank_constant,
        }) => {
            fingerprint.write_u64(1);
            fingerprint.write_u64(*window_size as u64);
            fingerprint.write_u64(*rank_constant as u64);
        }
        Some(FusionStrategy::WeightedBlend { weights }) => {
            fingerprint.write_u64(2);
            fingerprint.write_u64(weights.len() as u64);
            for weight in weights {
                fingerprint.write_u64(weight.to_bits() as u64);
            }
        }
    }
}

pub fn encode_predicate_tree(
    fingerprint: &mut StableFingerprintBuilder,
    tree: &paro_storage::index::PredicateTree,
) {
    use paro_storage::index::PredicateTree;

    match tree {
        PredicateTree::Leaf(predicate) => {
            fingerprint.write_u64(0);
            encode_predicate(fingerprint, predicate);
        }
        PredicateTree::And(children) => {
            fingerprint.write_u64(1);
            fingerprint.write_u64(children.len() as u64);
            for child in children {
                encode_predicate_tree(fingerprint, child);
            }
        }
        PredicateTree::Or(children) => {
            fingerprint.write_u64(2);
            fingerprint.write_u64(children.len() as u64);
            for child in children {
                encode_predicate_tree(fingerprint, child);
            }
        }
    }
}

pub fn encode_predicate(
    fingerprint: &mut StableFingerprintBuilder,
    predicate: &paro_storage::index::Predicate,
) {
    use paro_storage::index::{FixedMembershipWidth, Predicate};

    macro_rules! scalar_predicate {
        ($tag:expr, $column_id:expr, $value:expr) => {{
            fingerprint.write_u64($tag);
            fingerprint.write_u64(u64::from(*$column_id));
            super::scalar_identity::encode_value(fingerprint, $value);
        }};
    }

    match predicate {
        Predicate::Eq { column_id, value } => scalar_predicate!(0, column_id, value),
        Predicate::NotEq { column_id, value } => scalar_predicate!(1, column_id, value),
        Predicate::Lt { column_id, value } => scalar_predicate!(2, column_id, value),
        Predicate::Le { column_id, value } => scalar_predicate!(3, column_id, value),
        Predicate::Gt { column_id, value } => scalar_predicate!(4, column_id, value),
        Predicate::Ge { column_id, value } => scalar_predicate!(5, column_id, value),
        Predicate::In { column_id, values } => {
            fingerprint.write_u64(6);
            fingerprint.write_u64(u64::from(*column_id));
            fingerprint.write_u64(values.len() as u64);
            for value in values {
                super::scalar_identity::encode_value(fingerprint, value);
            }
        }
        Predicate::FixedIn { column_id, values } => {
            fingerprint.write_u64(7);
            fingerprint.write_u64(u64::from(*column_id));
            fingerprint.write_u64(values.len() as u64);
            let width = values.visit_canonical_values(|value| {
                fingerprint.write_bytes(&value.to_le_bytes());
            });
            fingerprint.write_u64(match width {
                FixedMembershipWidth::I32 => 0,
                FixedMembershipWidth::I64 => 1,
                FixedMembershipWidth::I128 => 2,
            });
        }
        Predicate::Range {
            column_id,
            lower,
            upper,
        } => {
            fingerprint.write_u64(8);
            fingerprint.write_u64(u64::from(*column_id));
            super::scalar_identity::encode_value(fingerprint, lower);
            super::scalar_identity::encode_value(fingerprint, upper);
        }
        Predicate::IsNull { column_id } => {
            fingerprint.write_u64(9);
            fingerprint.write_u64(u64::from(*column_id));
        }
        Predicate::IsNotNull { column_id } => {
            fingerprint.write_u64(10);
            fingerprint.write_u64(u64::from(*column_id));
        }
        Predicate::StringPrefix {
            column_id,
            prefix,
            negated,
        } => {
            fingerprint.write_u64(11);
            fingerprint.write_u64(u64::from(*column_id));
            fingerprint.write_bytes(prefix.as_bytes());
            fingerprint.write_u64(*negated as u64);
        }
        Predicate::StringPrefixIn {
            column_id,
            prefixes,
        } => {
            fingerprint.write_u64(12);
            fingerprint.write_u64(u64::from(*column_id));
            fingerprint.write_u64(prefixes.len() as u64);
            for prefix in prefixes {
                fingerprint.write_bytes(prefix.as_bytes());
            }
        }
        Predicate::StringLike {
            column_id,
            pattern,
            negated,
        } => {
            fingerprint.write_u64(13);
            fingerprint.write_u64(u64::from(*column_id));
            fingerprint.write_bytes(pattern.as_bytes());
            fingerprint.write_u64(*negated as u64);
        }
        Predicate::ColumnComparison {
            left_column_id,
            right_column_id,
            comparison,
        } => {
            fingerprint.write_u64(14);
            fingerprint.write_u64(u64::from(*left_column_id));
            fingerprint.write_u64(u64::from(*right_column_id));
            fingerprint.write_u64(match comparison {
                paro_storage::index::PredicateComparison::Equal => 0,
                paro_storage::index::PredicateComparison::NotEqual => 1,
                paro_storage::index::PredicateComparison::LessThan => 2,
                paro_storage::index::PredicateComparison::LessThanOrEqual => 3,
                paro_storage::index::PredicateComparison::GreaterThan => 4,
                paro_storage::index::PredicateComparison::GreaterThanOrEqual => 5,
            });
        }
    }
}

pub fn encode_optional_usize(fingerprint: &mut StableFingerprintBuilder, value: Option<usize>) {
    match value {
        None => fingerprint.write_u64(0),
        Some(value) => {
            fingerprint.write_u64(1);
            fingerprint.write_u64(value as u64);
        }
    }
}

pub fn encode_optional_string(fingerprint: &mut StableFingerprintBuilder, value: Option<&str>) {
    match value {
        None => fingerprint.write_u64(0),
        Some(value) => {
            fingerprint.write_u64(1);
            fingerprint.write_bytes(value.as_bytes());
        }
    }
}

pub fn encode_usizes(fingerprint: &mut StableFingerprintBuilder, values: &[usize]) {
    fingerprint.write_u64(values.len() as u64);
    for value in values {
        fingerprint.write_u64(*value as u64);
    }
}

pub fn encode_u32s(fingerprint: &mut StableFingerprintBuilder, values: &[u32]) {
    fingerprint.write_u64(values.len() as u64);
    for value in values {
        fingerprint.write_u64(u64::from(*value));
    }
}
