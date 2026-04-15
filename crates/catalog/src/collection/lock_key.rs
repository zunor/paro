// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::{CatalogCollection, CatalogEntryMap};
use crate::entry::CatalogObjectId;
use paro_common::error::{self as paro_error, Result};
use std::sync::Arc;

/// Stable collection scope used for cross-collection lock ordering.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum CollectionScope {
    DatabaseRoot,
    Schema(CatalogObjectId),
}

/// Stable family ordering for `SchemaContents` collections.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum CollectionFamily {
    Schemas,
    Tables,
    Views,
    Indexes,
    PropertyGraphs,
    Functions,
    TableFunctions,
    CopyFunctions,
    Sequences,
    Types,
    Collations,
}

/// Stable lock identity for a catalog collection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CollectionLockKey {
    scope: CollectionScope,
    family: CollectionFamily,
}

impl CollectionLockKey {
    fn new(scope: CollectionScope, family: CollectionFamily) -> Result<Self> {
        let valid = matches!(
            (scope, family),
            (CollectionScope::DatabaseRoot, CollectionFamily::Schemas)
        ) || matches!(scope, CollectionScope::Schema(_))
            && !matches!(family, CollectionFamily::Schemas);
        if !valid {
            return Err(paro_error::internal(format!(
                "invalid collection lock key combination: scope={scope:?}, family={family:?}"
            )));
        }
        Ok(Self { scope, family })
    }

    pub fn database_schemas() -> Self {
        Self::new(CollectionScope::DatabaseRoot, CollectionFamily::Schemas)
            .expect("database schemas lock key must be valid")
    }

    pub fn schema_family(schema_id: CatalogObjectId, family: CollectionFamily) -> Self {
        Self::new(CollectionScope::Schema(schema_id), family)
            .expect("schema family lock key must be valid")
    }
}

#[derive(Debug, Clone, Copy)]
pub(super) enum OrderedCollectionPair<'a> {
    One,
    Two {
        first: &'a Arc<CatalogCollection>,
        second: &'a Arc<CatalogCollection>,
    },
}

pub(super) fn ordered_collection_pair<'a>(
    source: &'a Arc<CatalogCollection>,
    target: &'a Arc<CatalogCollection>,
) -> Result<OrderedCollectionPair<'a>> {
    if Arc::ptr_eq(source, target) {
        return Ok(OrderedCollectionPair::One);
    }

    match source.lock_key().cmp(&target.lock_key()) {
        std::cmp::Ordering::Less => Ok(OrderedCollectionPair::Two {
            first: source,
            second: target,
        }),
        std::cmp::Ordering::Greater => Ok(OrderedCollectionPair::Two {
            first: target,
            second: source,
        }),
        std::cmp::Ordering::Equal => Err(paro_error::internal(format!(
            "duplicate collection lock key {:?} between \"{}\" and \"{}\"",
            source.lock_key(),
            source.catalog_name(),
            target.catalog_name()
        ))),
    }
}

pub(super) fn with_ordered_collection_maps<T, F>(
    source: &Arc<CatalogCollection>,
    target: &Arc<CatalogCollection>,
    f: F,
) -> Result<T>
where
    F: FnOnce(&mut CatalogEntryMap, &mut CatalogEntryMap) -> Result<T>,
{
    let OrderedCollectionPair::Two { first, second } = ordered_collection_pair(source, target)?
    else {
        return Err(paro_error::internal(
            "ordered collection helper expected two distinct collections",
        ));
    };

    let _first_lock = first
        .catalog_lock
        .lock()
        .map_err(|_| paro_error::internal("lock poisoned"))?;
    let _second_lock = second
        .catalog_lock
        .lock()
        .map_err(|_| paro_error::internal("lock poisoned"))?;
    let mut first_map = first
        .map
        .write()
        .map_err(|_| paro_error::internal("lock poisoned"))?;
    let mut second_map = second
        .map
        .write()
        .map_err(|_| paro_error::internal("lock poisoned"))?;

    if Arc::ptr_eq(first, source) {
        f(&mut first_map, &mut second_map)
    } else {
        f(&mut second_map, &mut first_map)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn database_root_orders_before_schema_scopes() {
        let root = CollectionLockKey::database_schemas();
        let schema = CollectionLockKey::schema_family(
            CatalogObjectId::from_raw(7),
            CollectionFamily::Tables,
        );
        assert!(root < schema);
    }

    #[test]
    fn schema_scope_orders_by_object_id_before_family() {
        let lower_schema = CollectionLockKey::schema_family(
            CatalogObjectId::from_raw(7),
            CollectionFamily::Collations,
        );
        let higher_schema = CollectionLockKey::schema_family(
            CatalogObjectId::from_raw(9),
            CollectionFamily::Tables,
        );
        assert!(lower_schema < higher_schema);
    }

    #[test]
    fn family_variant_order_is_the_lock_order_contract() {
        let schema_id = CatalogObjectId::from_raw(7);
        let ordered = [
            CollectionFamily::Tables,
            CollectionFamily::Views,
            CollectionFamily::Indexes,
            CollectionFamily::PropertyGraphs,
            CollectionFamily::Functions,
            CollectionFamily::TableFunctions,
            CollectionFamily::CopyFunctions,
            CollectionFamily::Sequences,
            CollectionFamily::Types,
            CollectionFamily::Collations,
        ];

        for pair in ordered.windows(2) {
            let lhs = CollectionLockKey::schema_family(schema_id, pair[0]);
            let rhs = CollectionLockKey::schema_family(schema_id, pair[1]);
            assert!(lhs < rhs, "{:?} should sort before {:?}", pair[0], pair[1]);
        }
    }

    #[test]
    fn invalid_schema_plus_schemas_combination_is_rejected() {
        let err = CollectionLockKey::new(
            CollectionScope::Schema(CatalogObjectId::from_raw(7)),
            CollectionFamily::Schemas,
        )
        .expect_err("schema-local schemas family must be rejected");
        assert!(err.to_string().contains("invalid collection lock key"));
    }

    #[test]
    fn ordered_pair_is_stable_regardless_of_call_direction() {
        let lower = CatalogCollection::new_for_tests("test", 1, CollectionFamily::Tables);
        let higher = CatalogCollection::new_for_tests("test", 2, CollectionFamily::Tables);

        let OrderedCollectionPair::Two {
            first: forward_first,
            second: forward_second,
        } = ordered_collection_pair(&lower, &higher).expect("forward ordering")
        else {
            panic!("distinct collections should require two locks");
        };
        let OrderedCollectionPair::Two {
            first: reverse_first,
            second: reverse_second,
        } = ordered_collection_pair(&higher, &lower).expect("reverse ordering")
        else {
            panic!("distinct collections should require two locks");
        };

        assert!(Arc::ptr_eq(forward_first, &lower));
        assert!(Arc::ptr_eq(forward_second, &higher));
        assert!(Arc::ptr_eq(reverse_first, &lower));
        assert!(Arc::ptr_eq(reverse_second, &higher));
    }

    #[test]
    fn ordered_pair_collapses_same_collection_to_one_lock() {
        let set = CatalogCollection::new_for_tests("test", 1, CollectionFamily::Tables);
        assert!(matches!(
            ordered_collection_pair(&set, &set).expect("same collection ordering"),
            OrderedCollectionPair::One
        ));
    }

    #[test]
    fn duplicate_lock_keys_on_distinct_collections_are_rejected() {
        let first = CatalogCollection::new_for_tests("test", 1, CollectionFamily::Tables);
        let second = CatalogCollection::new_for_tests("test", 1, CollectionFamily::Tables);

        let err = ordered_collection_pair(&first, &second)
            .expect_err("duplicate lock keys across distinct collections must be rejected");
        assert!(err.to_string().contains("duplicate collection lock key"));
    }
}
