//! Query-scoped immutable column identity and unordered Memo group schemas.

use std::collections::{BTreeMap, BTreeSet};

use paro_common::error::{self as paro_error, Result};
use paro_common::types::LogicalType;

use super::ids::{ColumnId, Fingerprint};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ColumnVisibility {
    Visible,
    Hidden,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ColumnOrigin {
    Base {
        relation: Fingerprint,
        catalog_column: u32,
    },
    Derived {
        key: Fingerprint,
    },
    Internal {
        key: Fingerprint,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ColumnDesc {
    pub id: ColumnId,
    pub logical_type: LogicalType,
    pub nullable: bool,
    pub origin: ColumnOrigin,
    pub visibility: ColumnVisibility,
    pub name_hint: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub struct ColumnCatalog {
    columns: Vec<ColumnDesc>,
    by_origin: BTreeMap<ColumnOrigin, ColumnId>,
}

impl ColumnCatalog {
    /// Intern a semantic column identity. Rules cannot allocate anonymous
    /// temporary columns: the normalized origin key owns the ID.
    pub fn intern(
        &mut self,
        logical_type: LogicalType,
        nullable: bool,
        origin: ColumnOrigin,
        visibility: ColumnVisibility,
        name_hint: Option<String>,
    ) -> Result<ColumnId> {
        if let Some(id) = self.by_origin.get(&origin).copied() {
            let existing = &self.columns[id.index()];
            if existing.logical_type != logical_type
                || existing.nullable != nullable
                || existing.visibility != visibility
            {
                return Err(paro_error::internal(
                    "the same column origin was interned with an incompatible contract",
                ));
            }
            return Ok(id);
        }
        let id = ColumnId::new(self.columns.len());
        self.columns.push(ColumnDesc {
            id,
            logical_type,
            nullable,
            origin,
            visibility,
            name_hint,
        });
        self.by_origin.insert(origin, id);
        Ok(id)
    }

    pub fn get(&self, id: ColumnId) -> Option<&ColumnDesc> {
        self.columns.get(id.index())
    }

    pub fn len(&self) -> usize {
        self.columns.len()
    }

    pub fn is_empty(&self) -> bool {
        self.columns.is_empty()
    }
}

/// Memo identity is a set/map keyed by immutable `ColumnId`; presentation
/// order and aliases are intentionally outside this contract.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct GroupSchema {
    columns: Box<[GroupColumn]>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct GroupColumn {
    pub id: ColumnId,
    pub logical_type: LogicalType,
    pub nullable: bool,
}

impl GroupSchema {
    pub fn new(columns: impl IntoIterator<Item = ColumnDesc>) -> Result<Self> {
        let mut by_id = BTreeMap::new();
        for column in columns {
            let contract = GroupColumn {
                id: column.id,
                logical_type: column.logical_type,
                nullable: column.nullable,
            };
            if by_id.insert(contract.id, contract).is_some() {
                return Err(paro_error::internal(
                    "Memo group schema contains a duplicate ColumnId",
                ));
            }
        }
        Ok(Self {
            columns: by_id.into_values().collect::<Vec<_>>().into_boxed_slice(),
        })
    }

    pub fn columns(&self) -> &[GroupColumn] {
        &self.columns
    }

    pub fn ids(&self) -> BTreeSet<ColumnId> {
        self.columns.iter().map(|column| column.id).collect()
    }

    pub fn contains(&self, id: ColumnId) -> bool {
        self.columns
            .binary_search_by_key(&id, |column| column.id)
            .is_ok()
    }

    pub fn validate_against(&self, catalog: &ColumnCatalog) -> Result<()> {
        for column in &self.columns {
            let Some(canonical) = catalog.get(column.id) else {
                return Err(paro_error::internal(format!(
                    "Memo group references unknown ColumnId {:?}",
                    column.id
                )));
            };
            if canonical.logical_type != column.logical_type
                || canonical.nullable != column.nullable
            {
                return Err(paro_error::internal(format!(
                    "Memo group changes the type/nullability contract of {:?}",
                    column.id
                )));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn group_schema_is_unordered_but_type_sensitive() {
        let mut catalog = ColumnCatalog::default();
        let a = catalog
            .intern(
                LogicalType::Integer,
                false,
                ColumnOrigin::Derived {
                    key: Fingerprint(1),
                },
                ColumnVisibility::Visible,
                Some("a".into()),
            )
            .unwrap();
        let b = catalog
            .intern(
                LogicalType::Varchar,
                true,
                ColumnOrigin::Derived {
                    key: Fingerprint(2),
                },
                ColumnVisibility::Visible,
                Some("b".into()),
            )
            .unwrap();

        let left = GroupSchema::new([
            catalog.get(a).unwrap().clone(),
            catalog.get(b).unwrap().clone(),
        ])
        .unwrap();
        let right = GroupSchema::new([
            catalog.get(b).unwrap().clone(),
            catalog.get(a).unwrap().clone(),
        ])
        .unwrap();
        assert_eq!(left, right);
        left.validate_against(&catalog).unwrap();

        let incompatible = GroupSchema::new([ColumnDesc {
            id: a,
            logical_type: LogicalType::BigInt,
            nullable: false,
            origin: ColumnOrigin::Derived {
                key: Fingerprint(1),
            },
            visibility: ColumnVisibility::Visible,
            name_hint: Some("a".into()),
        }])
        .unwrap();
        assert!(incompatible.validate_against(&catalog).is_err());
    }

    #[test]
    fn duplicate_column_id_is_rejected() {
        let column = ColumnDesc {
            id: ColumnId(1),
            logical_type: LogicalType::Integer,
            nullable: false,
            origin: ColumnOrigin::Internal {
                key: Fingerprint(9),
            },
            visibility: ColumnVisibility::Hidden,
            name_hint: None,
        };
        assert!(GroupSchema::new([column.clone(), column]).is_err());
    }

    #[test]
    fn normalized_origin_deterministically_reuses_identity() {
        let mut catalog = ColumnCatalog::default();
        let origin = ColumnOrigin::Internal {
            key: Fingerprint(42),
        };
        let first = catalog
            .intern(
                LogicalType::BigInt,
                false,
                origin,
                ColumnVisibility::Hidden,
                None,
            )
            .unwrap();
        let second = catalog
            .intern(
                LogicalType::BigInt,
                false,
                origin,
                ColumnVisibility::Hidden,
                Some("ignored-name-hint".into()),
            )
            .unwrap();
        assert_eq!(first, second);
        assert_eq!(catalog.len(), 1);
    }
}
