// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Output schema carried by a physical plan node.

use paro_common::types::LogicalType;

/// Stable SQL-facing identity of one physical output column.
///
/// Execution expressions address columns by ordinal, while EXPLAIN needs a
/// semantic name. Keeping that distinction explicit prevents optimizer-only
/// names (and rendered expression text) from leaking into the public plan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ColumnIdentity {
    Visible {
        name: String,
        qualifier: Option<Box<[String]>>,
    },
    Internal,
    InternalNamed(String),
}

impl ColumnIdentity {
    pub fn visible(name: impl Into<String>) -> Self {
        Self::Visible {
            name: name.into(),
            qualifier: None,
        }
    }

    pub fn qualified(name: impl Into<String>, qualifier: impl Into<String>) -> Self {
        Self::Visible {
            name: name.into(),
            qualifier: Some(vec![qualifier.into()].into_boxed_slice()),
        }
    }

    pub fn qualified_path(
        name: impl Into<String>,
        qualifier: impl IntoIterator<Item = String>,
    ) -> Self {
        let qualifier = qualifier.into_iter().collect::<Vec<_>>();
        assert!(
            !qualifier.is_empty(),
            "column qualifier path cannot be empty"
        );
        Self::Visible {
            name: name.into(),
            qualifier: Some(qualifier.into_boxed_slice()),
        }
    }

    pub fn internal_named(name: impl Into<String>) -> Self {
        Self::InternalNamed(name.into())
    }

    pub fn is_internal(&self) -> bool {
        matches!(self, Self::Internal | Self::InternalNamed(_))
    }

    pub fn unqualified_name(&self, ordinal: usize) -> String {
        match self {
            Self::Visible { name, .. } => name.clone(),
            Self::Internal => format!("__internal_{}", ordinal + 1),
            Self::InternalNamed(name) => name.clone(),
        }
    }

    pub fn qualified_name(&self, ordinal: usize) -> String {
        match self {
            Self::Visible {
                name,
                qualifier: Some(qualifier),
            } => format!("{}.{name}", qualifier.join(".")),
            _ => self.unqualified_name(ordinal),
        }
    }

    pub fn without_qualifier(&self) -> Self {
        match self {
            Self::Visible { name, .. } => Self::visible(name.clone()),
            Self::Internal => Self::Internal,
            Self::InternalNamed(name) => Self::InternalNamed(name.clone()),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct RowType {
    pub names: Box<[String]>,
    pub types: Box<[LogicalType]>,
    pub identities: Box<[ColumnIdentity]>,
}

impl RowType {
    pub fn new(names: Vec<String>, types: Vec<LogicalType>) -> Self {
        let identities = names
            .iter()
            .cloned()
            .map(ColumnIdentity::visible)
            .collect::<Vec<_>>();
        Self::with_identities(names, types, identities)
    }

    pub fn with_identities(
        names: Vec<String>,
        types: Vec<LogicalType>,
        identities: Vec<ColumnIdentity>,
    ) -> Self {
        assert_eq!(
            names.len(),
            types.len(),
            "physical row type names/types must stay aligned"
        );
        assert_eq!(
            identities.len(),
            types.len(),
            "physical row type identities/types must stay aligned"
        );
        Self {
            names: names.into_boxed_slice(),
            types: types.into_boxed_slice(),
            identities: identities.into_boxed_slice(),
        }
    }

    pub fn explain_names(&self, qualified: bool) -> Vec<String> {
        self.identities
            .iter()
            .enumerate()
            .map(|(ordinal, identity)| {
                if qualified {
                    identity.qualified_name(ordinal)
                } else {
                    identity.unqualified_name(ordinal)
                }
            })
            .collect()
    }

    #[inline]
    pub fn column_count(&self) -> usize {
        self.types.len()
    }
}
