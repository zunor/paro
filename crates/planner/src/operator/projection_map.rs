// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Explicit projections over a logical child layout.

/// Columns retained from one logical child.
///
/// `All` is layout-relative and therefore survives structural optimizer passes
/// that compact or replace the child. `Columns([])` is the distinct, exact
/// zero-column projection. Positional maps are resolved only when an operator
/// deliberately selects columns or when physical lowering freezes the final
/// logical layout.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProjectionMap {
    All,
    Columns(Vec<usize>),
}

impl ProjectionMap {
    pub const fn all() -> Self {
        Self::All
    }

    pub const fn none() -> Self {
        Self::Columns(Vec::new())
    }

    pub fn new(indices: Vec<usize>) -> Self {
        Self::Columns(indices)
    }

    pub fn as_columns(&self) -> Option<&[usize]> {
        match self {
            Self::All => None,
            Self::Columns(indices) => Some(indices),
        }
    }

    pub fn to_indices(&self, child_width: usize) -> Vec<usize> {
        match self {
            Self::All => (0..child_width).collect(),
            Self::Columns(indices) => {
                debug_assert!(
                    indices.iter().all(|index| *index < child_width),
                    "projection index must be within the child layout"
                );
                indices.clone()
            }
        }
    }

    pub fn clear(&mut self) {
        *self = Self::none();
    }

    /// Append a child column when this is an exact projection and the column
    /// is not already present. `All` already includes every current and future
    /// child column.
    pub fn include(&mut self, index: usize) {
        if let Self::Columns(indices) = self {
            if !indices.contains(&index) {
                indices.push(index);
            }
        }
    }

    pub const fn is_all(&self) -> bool {
        matches!(self, Self::All)
    }

    pub fn is_none(&self) -> bool {
        matches!(self, Self::Columns(indices) if indices.is_empty())
    }

    pub fn is_identity(&self, child_width: usize) -> bool {
        match self {
            Self::All => true,
            Self::Columns(indices) => {
                debug_assert!(
                    indices.iter().all(|index| *index < child_width),
                    "projection index must be within the child layout"
                );
                indices.len() == child_width && indices.iter().copied().eq(0..child_width)
            }
        }
    }
}

impl From<Vec<usize>> for ProjectionMap {
    fn from(indices: Vec<usize>) -> Self {
        Self::new(indices)
    }
}

#[cfg(test)]
mod tests {
    use super::ProjectionMap;

    #[test]
    fn all_and_zero_column_projections_are_distinct() {
        assert!(ProjectionMap::all().is_all());
        assert_eq!(ProjectionMap::all().to_indices(3), vec![0, 1, 2]);
        assert!(ProjectionMap::none().is_none());
        assert!(!ProjectionMap::none().is_all());
    }
}
